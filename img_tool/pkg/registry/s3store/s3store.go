// Copyright 2026 The rules_img Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package s3store is a registry.Store whose manifests and tags survive a
// restart: an in-memory Store serves every read, and every mutation is written
// through to S3 before it returns. Open rebuilds the in-memory copy from S3.
//
// It assumes it is the only writer under its prefix. Two registries sharing a
// prefix would each serve its own memory and overwrite the other's objects.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/bazel-contrib/rules_img/img_tool/pkg/registry"
)

// Client is the subset of *s3.Client the store uses.
type Client interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	s3.ListObjectsV2APIClient
}

const (
	manifestsDir = "manifests/"
	tagsDir      = "tags/"
	kindMetadata = "kind"
)

// Config tunes how hard a write-through tries before giving up on S3.
type Config struct {
	// Attempts is how many times one S3 write is tried. Defaults to 5.
	Attempts int
	// Backoff is the wait before the second attempt; it doubles after that.
	// Defaults to 200ms.
	Backoff time.Duration
	// Timeout bounds one S3 call. Defaults to 30s.
	Timeout time.Duration
	// LoadParallelism is how many manifests Open fetches at once. Defaults to 16.
	LoadParallelism int
}

func (c *Config) defaults() {
	if c.Attempts <= 0 {
		c.Attempts = 5
	}
	if c.Backoff <= 0 {
		c.Backoff = 200 * time.Millisecond
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.LoadParallelism <= 0 {
		c.LoadParallelism = 16
	}
}

// Store implements registry.Store over an in-memory Store and an S3 prefix.
type Store struct {
	mem    registry.Store
	client Client
	bucket string
	prefix string
	cfg    Config

	failMu   sync.Mutex
	failures int
}

var _ registry.Store = (*Store)(nil)

// Open loads every manifest and tag stored under bucket/prefix and returns a
// Store that writes subsequent changes back there. A prefix of "" uses the
// bucket root; otherwise a trailing "/" is added if missing.
func Open(ctx context.Context, client Client, bucket, prefix string, cfg Config) (*Store, error) {
	if bucket == "" {
		return nil, errors.New("s3store: bucket is required")
	}
	cfg.defaults()
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	s := &Store{mem: registry.NewMemStore(), client: client, bucket: bucket, prefix: prefix, cfg: cfg}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// WriteFailures is the number of writes that exhausted their retries. Each one
// is state that will be missing after the next restart.
func (s *Store) WriteFailures() int {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	return s.failures
}

// ManifestKey is where a manifest lives: <prefix>manifests/<repo>/<algorithm>/<hex>.
func (s *Store) ManifestKey(repo string, digest v1.Hash) string {
	return s.prefix + manifestsDir + repo + "/" + digest.Algorithm + "/" + digest.Hex
}

// TagKey is where a tag lives: <prefix>tags/<repo>/<tag>. Its body is the digest.
func (s *Store) TagKey(repo, tag string) string {
	return s.prefix + tagsDir + repo + "/" + tag
}

// parseManifestKey inverts ManifestKey for a key with the prefix already removed.
func parseManifestKey(rel string) (string, v1.Hash, error) {
	rest, ok := strings.CutPrefix(rel, manifestsDir)
	if !ok {
		return "", v1.Hash{}, fmt.Errorf("not a manifest key: %q", rel)
	}
	parts := strings.Split(rest, "/")
	if len(parts) < 3 {
		return "", v1.Hash{}, fmt.Errorf("manifest key %q has no <repo>/<algorithm>/<hex>", rel)
	}
	repo := strings.Join(parts[:len(parts)-2], "/")
	h, err := v1.NewHash(parts[len(parts)-2] + ":" + parts[len(parts)-1])
	if err != nil {
		return "", v1.Hash{}, fmt.Errorf("manifest key %q: %w", rel, err)
	}
	return repo, h, nil
}

// parseTagKey inverts TagKey for a key with the prefix already removed.
func parseTagKey(rel string) (string, string, error) {
	rest, ok := strings.CutPrefix(rel, tagsDir)
	if !ok {
		return "", "", fmt.Errorf("not a tag key: %q", rel)
	}
	i := strings.LastIndex(rest, "/")
	if i <= 0 || i == len(rest)-1 {
		return "", "", fmt.Errorf("tag key %q has no <repo>/<tag>", rel)
	}
	return rest[:i], rest[i+1:], nil
}

func (s *Store) list(ctx context.Context, dir string) ([]string, error) {
	var keys []string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.prefix + dir),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: listing s3://%s/%s%s: %w", s.bucket, s.prefix, dir, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (s *Store) get(ctx context.Context, key string) ([]byte, *s3.GetObjectOutput, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, nil, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	return body, out, err
}

func (s *Store) load(ctx context.Context) error {
	manifestKeys, err := s.list(ctx, manifestsDir)
	if err != nil {
		return err
	}
	tagKeys, err := s.list(ctx, tagsDir)
	if err != nil {
		return err
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
		sem   = make(chan struct{}, s.cfg.LoadParallelism)
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil {
			first = err
		}
	}
	for _, key := range manifestKeys {
		repo, digest, err := parseManifestKey(strings.TrimPrefix(key, s.prefix))
		if err != nil {
			log.Printf("s3store: skipping %s: %v", key, err)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			body, out, err := s.get(ctx, key)
			if err != nil {
				fail(fmt.Errorf("s3store: reading %s: %w", key, err))
				return
			}
			// A manifest whose bytes no longer hash to its key would be served under a lie.
			if got, _, err := v1.SHA256(bytes.NewReader(body)); err != nil || got != digest {
				log.Printf("s3store: skipping %s: content hashes to %s", key, got)
				return
			}
			kind, _ := strconv.Atoi(out.Metadata[kindMetadata])
			s.mem.PutManifest(repo, digest, registry.Manifest{
				ContentType: aws.ToString(out.ContentType),
				Kind:        registry.Kind(kind),
				Blob:        body,
			})
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}

	for _, key := range tagKeys {
		repo, tag, err := parseTagKey(strings.TrimPrefix(key, s.prefix))
		if err != nil {
			log.Printf("s3store: skipping %s: %v", key, err)
			continue
		}
		body, _, err := s.get(ctx, key)
		if err != nil {
			return fmt.Errorf("s3store: reading %s: %w", key, err)
		}
		digest, err := v1.NewHash(strings.TrimSpace(string(body)))
		if err != nil {
			log.Printf("s3store: skipping %s: %v", key, err)
			continue
		}
		s.mem.PutTag(repo, tag, digest)
	}
	log.Printf("s3store: loaded %d manifest(s) and %d tag(s) from s3://%s/%s", len(manifestKeys), len(tagKeys), s.bucket, s.prefix)
	return nil
}

// retry runs op until it succeeds or the attempts run out, counting the loss.
func (s *Store) retry(what string, op func(ctx context.Context) error) {
	wait := s.cfg.Backoff
	var err error
	for attempt := 1; attempt <= s.cfg.Attempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		err = op(ctx)
		cancel()
		if err == nil {
			return
		}
		if attempt < s.cfg.Attempts {
			time.Sleep(wait)
			wait *= 2
		}
	}
	s.failMu.Lock()
	s.failures++
	s.failMu.Unlock()
	log.Printf("s3store: %s failed after %d attempt(s), lost on the next restart: %v", what, s.cfg.Attempts, err)
}

func (s *Store) put(key string, body []byte, contentType string, metadata map[string]string) {
	s.retry("PUT s3://"+s.bucket+"/"+key, func(ctx context.Context) error {
		in := &s3.PutObjectInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			Body:     bytes.NewReader(body),
			Metadata: metadata,
		}
		if contentType != "" {
			in.ContentType = aws.String(contentType)
		}
		_, err := s.client.PutObject(ctx, in)
		return err
	})
}

func (s *Store) del(key string) {
	s.retry("DELETE s3://"+s.bucket+"/"+key, func(ctx context.Context) error {
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
		var missing *s3types.NoSuchKey
		if errors.As(err, &missing) {
			return nil
		}
		return err
	})
}

func (s *Store) HasRepo(repo string) bool { return s.mem.HasRepo(repo) }

func (s *Store) RangeRepos(fn func(repo string) bool) { s.mem.RangeRepos(fn) }

func (s *Store) GetManifest(repo string, digest v1.Hash) (registry.Manifest, bool) {
	return s.mem.GetManifest(repo, digest)
}

// PutManifest is durable before it returns, so a 201 to the client means the
// manifest survives a restart. A tag is only ever written after its manifest.
func (s *Store) PutManifest(repo string, digest v1.Hash, manifest registry.Manifest) {
	s.put(s.ManifestKey(repo, digest), manifest.Blob, manifest.ContentType, map[string]string{
		kindMetadata: strconv.Itoa(int(manifest.Kind)),
	})
	s.mem.PutManifest(repo, digest, manifest)
}

func (s *Store) DeleteManifest(repo string, digest v1.Hash) {
	s.mem.DeleteManifest(repo, digest)
	s.del(s.ManifestKey(repo, digest))
}

func (s *Store) RangeManifests(repo string, fn func(digest v1.Hash, manifest registry.Manifest) bool) {
	s.mem.RangeManifests(repo, fn)
}

func (s *Store) ResolveTag(repo, tag string) (v1.Hash, bool) { return s.mem.ResolveTag(repo, tag) }

func (s *Store) PutTag(repo, tag string, digest v1.Hash) {
	s.put(s.TagKey(repo, tag), []byte(digest.String()), "text/plain", nil)
	s.mem.PutTag(repo, tag, digest)
}

func (s *Store) DeleteTag(repo, tag string) {
	s.mem.DeleteTag(repo, tag)
	s.del(s.TagKey(repo, tag))
}

func (s *Store) RangeTags(repo string, fn func(tag string, digest v1.Hash) bool) {
	s.mem.RangeTags(repo, fn)
}

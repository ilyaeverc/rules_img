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

package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/bazel-contrib/rules_img/img_tool/pkg/registry"
)

type object struct {
	body        []byte
	contentType *string
	metadata    map[string]string
}

// fakeS3 is a single-bucket, in-memory S3 with an optional run of injected put failures.
type fakeS3 struct {
	mu         sync.Mutex
	objects    map[string]object
	failPuts   int
	puts, dels int
}

func newFake() *fakeS3 { return &fakeS3{objects: map[string]object{}} }

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(o.body)), ContentType: o.contentType, Metadata: o.metadata}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPuts > 0 {
		f.failPuts--
		return nil, errors.New("injected failure")
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.puts++
	f.objects[aws.ToString(in.Key)] = object{body: body, contentType: in.ContentType, metadata: in.Metadata}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dels++
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

// ListObjectsV2 pages two keys at a time, so Open has to follow continuation tokens.
func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) && k > aws.ToString(in.ContinuationToken) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{}
	for i, k := range keys {
		if i == 2 {
			out.IsTruncated = aws.Bool(true)
			out.NextContinuationToken = aws.String(keys[1])
			break
		}
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	return out, nil
}

func manifest(t *testing.T, body string) (v1.Hash, registry.Manifest) {
	t.Helper()
	h, _, err := v1.SHA256(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return h, registry.Manifest{ContentType: "application/vnd.oci.image.manifest.v1+json", Kind: registry.KindManifest, Blob: []byte(body)}
}

func open(t *testing.T, f *fakeS3) *Store {
	t.Helper()
	s, err := Open(context.Background(), f, "bucket", "cas-registry", Config{Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRestartRestoresManifestsAndTags(t *testing.T) {
	f := newFake()
	s := open(t, f)
	// Nested repository names are the norm (team/app), so the key parser must keep the slashes.
	repos := []string{"mcp-gateway", "team/nested/app"}
	digests := map[string]v1.Hash{}
	for i, repo := range repos {
		d, m := manifest(t, `{"schemaVersion":2,"n":`+string(rune('0'+i))+`}`)
		s.PutManifest(repo, d, m)
		s.PutTag(repo, "20261007.143508-017a290", d)
		s.PutTag(repo, "latest", d)
		digests[repo] = d
	}

	restarted := open(t, f)
	for _, repo := range repos {
		got, ok := restarted.ResolveTag(repo, "latest")
		if !ok || got != digests[repo] {
			t.Fatalf("%s:latest = %v, %v; want %v", repo, got, ok, digests[repo])
		}
		m, ok := restarted.GetManifest(repo, got)
		if !ok || m.Kind != registry.KindManifest || m.ContentType != "application/vnd.oci.image.manifest.v1+json" {
			t.Fatalf("%s@%s = %+v, %v", repo, got, m, ok)
		}
	}
}

func TestDeletesReachS3(t *testing.T) {
	f := newFake()
	s := open(t, f)
	d, m := manifest(t, `{"a":1}`)
	s.PutManifest("repo", d, m)
	s.PutTag("repo", "t", d)
	s.DeleteTag("repo", "t")
	s.DeleteManifest("repo", d)
	if len(f.objects) != 0 {
		t.Fatalf("objects left after deleting everything: %v", f.objects)
	}
	restarted := open(t, f)
	if restarted.HasRepo("repo") {
		t.Fatal("a deleted repository came back after a restart")
	}
}

func TestTransientPutFailuresAreRetried(t *testing.T) {
	f := newFake()
	s := open(t, f)
	f.failPuts = 2
	d, m := manifest(t, `{"b":2}`)
	s.PutManifest("repo", d, m)
	if s.WriteFailures() != 0 {
		t.Fatalf("WriteFailures = %d after a recoverable failure", s.WriteFailures())
	}
	if _, ok := open(t, f).GetManifest("repo", d); !ok {
		t.Fatal("manifest missing after restart")
	}
}

func TestExhaustedRetriesAreCountedButStillServed(t *testing.T) {
	f := newFake()
	s, err := Open(context.Background(), f, "bucket", "", Config{Attempts: 2, Backoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	f.failPuts = 2
	d, m := manifest(t, `{"c":3}`)
	s.PutManifest("repo", d, m)
	if s.WriteFailures() != 1 {
		t.Fatalf("WriteFailures = %d, want 1", s.WriteFailures())
	}
	if _, ok := s.GetManifest("repo", d); !ok {
		t.Fatal("a manifest S3 refused must still be served until restart")
	}
}

func TestCorruptManifestObjectIsSkipped(t *testing.T) {
	f := newFake()
	s := open(t, f)
	d, m := manifest(t, `{"d":4}`)
	s.PutManifest("repo", d, m)
	key := s.ManifestKey("repo", d)
	o := f.objects[key]
	o.body = []byte(`{"tampered":true}`)
	f.objects[key] = o
	if _, ok := open(t, f).GetManifest("repo", d); ok {
		t.Fatal("served a manifest whose bytes do not hash to its digest")
	}
}

func TestKeyParsing(t *testing.T) {
	repo, h, err := parseManifestKey("manifests/a/b/sha256/" + strings.Repeat("ab", 32))
	if err != nil || repo != "a/b" || h.Algorithm != "sha256" {
		t.Fatalf("parseManifestKey = %q, %v, %v", repo, h, err)
	}
	if _, _, err := parseManifestKey("manifests/sha256/abc"); err == nil {
		t.Fatal("a key with no repository parsed")
	}
	repo, tag, err := parseTagKey("tags/a/b/latest")
	if err != nil || repo != "a/b" || tag != "latest" {
		t.Fatalf("parseTagKey = %q, %q, %v", repo, tag, err)
	}
	if _, _, err := parseTagKey("tags/latest"); err == nil {
		t.Fatal("a tag key with no repository parsed")
	}
}

func TestRequiresBucket(t *testing.T) {
	if _, err := Open(context.Background(), newFake(), "", "p", Config{}); err == nil {
		t.Fatal("Open accepted an empty bucket")
	}
}

// TestRegistryServesTagsAfterRestart drives the real registry handlers: push an image
// through one instance, then pull it by tag from a second built on the same S3 objects.
func TestRegistryServesTagsAfterRestart(t *testing.T) {
	f := newFake()
	blobs := registry.NewInMemoryBlobHandler()
	img, err := random.Image(64, 2)
	if err != nil {
		t.Fatal(err)
	}

	first := httptest.NewServer(registry.New(registry.WithStore(open(t, f)), registry.WithBlobHandler(blobs)))
	ref, err := name.ParseReference(strings.TrimPrefix(first.URL, "http://") + "/team/app:v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second := httptest.NewServer(registry.New(registry.WithStore(open(t, f)), registry.WithBlobHandler(blobs)))
	defer second.Close()
	ref2, err := name.ParseReference(strings.TrimPrefix(second.URL, "http://") + "/team/app:v1")
	if err != nil {
		t.Fatal(err)
	}
	pulled, err := remote.Image(ref2)
	if err != nil {
		t.Fatalf("pull after restart: %v", err)
	}
	want, _ := img.Digest()
	got, _ := pulled.Digest()
	if got != want {
		t.Fatalf("digest after restart = %s, want %s", got, want)
	}
}

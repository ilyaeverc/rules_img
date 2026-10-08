package registry

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/bazel-contrib/rules_img/img_tool/pkg/registry"
	registryv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestSeedRestoresBlobSizesOfStoredManifests(t *testing.T) {
	layer := registryv1.Hash{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}
	config := registryv1.Hash{Algorithm: "sha256", Hex: strings.Repeat("b", 64)}
	image := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"config":{"mediaType":%q,"digest":%q,"size":7},"layers":[{"mediaType":%q,"digest":%q,"size":806}]}`,
		types.OCIManifestSchema1, types.OCIConfigJSON, config, types.OCILayer, layer))
	imageDigest, _, err := registryv1.SHA256(bytes.NewReader(image))
	if err != nil {
		t.Fatal(err)
	}
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"manifests":[{"mediaType":%q,"digest":%q,"size":%d}]}`,
		types.OCIImageIndex, types.OCIManifestSchema1, imageDigest, len(image)))
	indexDigest, _, err := registryv1.SHA256(bytes.NewReader(index))
	if err != nil {
		t.Fatal(err)
	}

	store := registry.NewMemStore()
	store.PutManifest("mcp-gateway", imageDigest, registry.Manifest{ContentType: string(types.OCIManifestSchema1), Blob: image})
	store.PutManifest("mcp-gateway", indexDigest, registry.Manifest{ContentType: string(types.OCIImageIndex), Blob: index})

	cache := NewBlobSizeCache()
	if seeded := cache.Seed(store); seeded != 2 {
		t.Fatalf("Seed() = %d, want 2", seeded)
	}
	for hash, want := range map[registryv1.Hash]int64{layer: 806, config: 7, imageDigest: int64(len(image)), indexDigest: int64(len(index))} {
		if got, ok := cache.Get(hash); !ok || got != want {
			t.Errorf("Get(%s) = %d, %v; want %d, true", hash, got, ok, want)
		}
	}
}

func TestSeedSkipsManifestsItCannotParse(t *testing.T) {
	store := registry.NewMemStore()
	broken := []byte("{not json")
	digest, _, err := registryv1.SHA256(bytes.NewReader(broken))
	if err != nil {
		t.Fatal(err)
	}
	store.PutManifest("mcp-gateway", digest, registry.Manifest{ContentType: string(types.OCIManifestSchema1), Blob: broken})

	cache := NewBlobSizeCache()
	if seeded := cache.Seed(store); seeded != 1 {
		t.Fatalf("Seed() = %d, want 1", seeded)
	}
	if got, ok := cache.Get(digest); !ok || got != int64(len(broken)) {
		t.Errorf("Get(manifest) = %d, %v; want %d, true", got, ok, len(broken))
	}
}

package blob

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPathStoreUsesSubdirectories(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := NewPathStore(root)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	id, _, err := store.Put(ctx, strings.NewReader("subdir-test"), int64(len("subdir-test")), PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	name := string(id)
	if len(name) < 4 {
		t.Fatalf("unexpected shard id %q", name)
	}
	expected := filepath.Join(root, name[:2], name[2:4], name)
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("expected shard at %s: %v", expected, err)
	}
}

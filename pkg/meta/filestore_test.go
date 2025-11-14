package meta

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
)

func TestFileStorePersistsData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	storePath := filepath.Join(dir, "meta.json")
	store, err := NewFileStore(storePath)
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	id, err := store.AllocateID(ctx)
	if err != nil {
		t.Fatalf("alloc id: %v", err)
	}
	inode := Inode{
		ID:        id,
		Parent:    RootID,
		Name:      "foo",
		Parents:   parentMap(RootID, "foo"),
		Type:      TypeFile,
		Mode:      0o644,
		UID:       0,
		GID:       0,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
	}
	if err := store.Put(ctx, inode); err != nil {
		t.Fatalf("put: %v", err)
	}
	reopened, err := NewFileStore(storePath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "foo" {
		t.Fatalf("expected foo, got %s", got.Name)
	}
	kids, err := reopened.Children(ctx, RootID)
	if err != nil {
		t.Fatalf("children: %v", err)
	}
	if len(kids) != 1 {
		t.Fatalf("expected 1 child, got %d", len(kids))
	}
}

func parentMap(parent fs.ID, name string) map[fs.ID]map[string]struct{} {
	return map[fs.ID]map[string]struct{}{
		parent: {
			name: struct{}{},
		},
	}
}

package meta

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
)

func TestBoltStoreBasicCRUD(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewBoltStore(BoltConfig{Path: path})
	if err != nil {
		t.Fatalf("new bolt store: %v", err)
	}
	defer store.Close()

	root, err := store.Root(ctx)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	if root.ID != rootInodeID {
		t.Fatalf("unexpected root id %s", root.ID)
	}

	id, err := store.AllocateID(ctx)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	inode := Inode{
		ID:        id,
		Parent:    root.ID,
		Name:      "foo.txt",
		Parents:   map[fs.ID]map[string]struct{}{root.ID: {"foo.txt": struct{}{}}},
		Type:      TypeFile,
		Mode:      0o644,
		UID:       1000,
		GID:       1000,
		Size:      11,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
		Metadata:  map[string]string{},
	}
	if err := store.Put(ctx, inode); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "foo.txt" || got.Size != 11 {
		t.Fatalf("unexpected inode %+v", got)
	}
	children, err := store.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children: %v", err)
	}
	if len(children) != 1 || children[0].Name != "foo.txt" {
		t.Fatalf("unexpected children %+v", children)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.Get(ctx, id); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestBoltStoreShardRefs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewBoltStore(BoltConfig{Path: path})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	if refs, err := store.IncRef(ctx, "shard-a", 1); err != nil || refs != 1 {
		t.Fatalf("inc: refs=%d err=%v", refs, err)
	}
	if err := store.DecideGC(ctx, "shard-a", 1); err != nil {
		t.Fatalf("decide gc: %v", err)
	}
	if refs, err := store.IncRef(ctx, "shard-a", -1); err != nil || refs != 0 {
		t.Fatalf("dec refs=%d err=%v", refs, err)
	}
	if err := store.DecideGC(ctx, "shard-a", 0); err != nil {
		t.Fatalf("decide zero: %v", err)
	}
	pending, err := store.ListZeroRef(ctx, 10)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) != 1 || pending[0] != "shard-a" {
		t.Fatalf("unexpected pending %v", pending)
	}
	if err := store.MarkGCComplete(ctx, "shard-a"); err != nil {
		t.Fatalf("mark complete: %v", err)
	}
	pending, err = store.ListZeroRef(ctx, 10)
	if err != nil {
		t.Fatalf("list zero after: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty pending, got %v", pending)
	}
}

func TestMigrateToBolt(t *testing.T) {
	ctx := context.Background()
	mem := NewMemoryStore()
	root, err := mem.Root(ctx)
	if err != nil {
		t.Fatalf("mem root: %v", err)
	}
	childID, err := mem.AllocateID(ctx)
	if err != nil {
		t.Fatalf("alloc child: %v", err)
	}
	child := Inode{
		ID:   childID,
		Type: TypeFile,
		Size: 5,
		Mode: 0o600,
		UID:  2000,
		GID:  3000,
		Parents: map[fs.ID]map[string]struct{}{
			root.ID: map[string]struct{}{"bar": {}},
		},
		LinkCount: 1,
		Shards: []ShardRef{
			{ShardID: "s-1", Size: 5},
		},
	}
	if err := mem.Put(ctx, child); err != nil {
		t.Fatalf("mem put: %v", err)
	}
	path := filepath.Join(t.TempDir(), "bolt.db")
	dst, err := MigrateToBolt(ctx, mem, BoltConfig{Path: path})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	defer dst.Close()
	got, err := dst.Get(ctx, child.ID)
	if err != nil {
		t.Fatalf("dst get: %v", err)
	}
	if got.UID != child.UID || got.GID != child.GID {
		t.Fatalf("unexpected inode %+v", got)
	}
	pending, err := dst.ListZeroRef(ctx, 10)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending gc, got %v", pending)
	}
	if _, err := dst.IncRef(ctx, "s-1", 0); err != nil {
		t.Fatalf("inc ref zero err=%v", err)
	}
}

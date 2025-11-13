package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMemoryStoreGCQueue(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if _, err := store.IncRef(ctx, "shard-a", 1); err != nil {
		t.Fatalf("inc ref: %v", err)
	}
	if refs, err := store.IncRef(ctx, "shard-a", -1); err != nil || refs != 0 {
		t.Fatalf("dec ref refs=%d err=%v", refs, err)
	}
	if err := store.DecideGC(ctx, "shard-a", 0); err != nil {
		t.Fatalf("decide gc: %v", err)
	}
	pending, err := store.ListZeroRef(ctx, 10)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) != 1 || pending[0] != "shard-a" {
		t.Fatalf("expected shard-a pending, got %v", pending)
	}
	if err := store.MarkGCComplete(ctx, "shard-a"); err != nil {
		t.Fatalf("mark complete: %v", err)
	}
	pending, err = store.ListZeroRef(ctx, 1)
	if err != nil {
		t.Fatalf("list zero second: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending shards, got %v", pending)
	}
}

func TestFileStoreGCPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	if _, err := store.IncRef(ctx, "shard-b", 1); err != nil {
		t.Fatalf("inc ref: %v", err)
	}
	if refs, err := store.IncRef(ctx, "shard-b", -1); err != nil || refs != 0 {
		t.Fatalf("dec ref refs=%d err=%v", refs, err)
	}
	if err := store.DecideGC(ctx, "shard-b", 0); err != nil {
		t.Fatalf("decide gc: %v", err)
	}
	pending, err := store.ListZeroRef(ctx, 1)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) != 1 || pending[0] != "shard-b" {
		t.Fatalf("expected shard-b pending, got %v", pending)
	}
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	pending, err = reopened.ListZeroRef(ctx, 1)
	if err != nil {
		t.Fatalf("list zero reopened: %v", err)
	}
	if len(pending) != 1 || pending[0] != "shard-b" {
		t.Fatalf("expected shard-b pending after reopen, got %v", pending)
	}
	if err := reopened.MarkGCComplete(ctx, "shard-b"); err != nil {
		t.Fatalf("mark complete: %v", err)
	}
	pending, err = reopened.ListZeroRef(ctx, 1)
	if err != nil {
		t.Fatalf("list zero final: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty pending after GC, got %v", pending)
	}
}

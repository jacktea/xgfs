package gc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/meta"
)

func TestSweeperRemovesPendingShards(t *testing.T) {
	ctx := context.Background()
	store := meta.NewMemoryStore()
	stub := &stubBlobStore{}
	if _, err := store.IncRef(ctx, "shard-1", 1); err != nil {
		t.Fatalf("inc ref: %v", err)
	}
	if refs, err := store.IncRef(ctx, "shard-1", -1); err != nil || refs != 0 {
		t.Fatalf("dec ref refs=%d err=%v", refs, err)
	}
	if err := store.DecideGC(ctx, "shard-1", 0); err != nil {
		t.Fatalf("decide gc: %v", err)
	}

	sweeper := NewSweeper(Options{
		Store:     store,
		Blob:      stub,
		BatchSize: 1,
		Logger:    func(string, ...any) {},
	})
	count, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 deletion, got %d", count)
	}
	if len(stub.deleted) != 1 || stub.deleted[0] != blob.ID("shard-1") {
		t.Fatalf("expected shard-1 deleted, got %v", stub.deleted)
	}
	pending, err := store.ListZeroRef(ctx, 1)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty pending queue, got %v", pending)
	}
}

type stubBlobStore struct {
	mu      sync.Mutex
	deleted []blob.ID
}

func (s *stubBlobStore) Put(context.Context, io.Reader, int64, blob.PutOptions) (blob.ID, int64, error) {
	return "", 0, errors.New("put not supported")
}

func (s *stubBlobStore) Get(context.Context, blob.ID) (io.ReadCloser, int64, error) {
	return nil, 0, errors.New("get not supported")
}

func (s *stubBlobStore) Delete(ctx context.Context, id blob.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, id)
	return nil
}

func (s *stubBlobStore) Exists(context.Context, blob.ID) (bool, error) {
	return false, fmt.Errorf("exists not supported")
}

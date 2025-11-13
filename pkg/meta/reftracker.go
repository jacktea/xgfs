package meta

import (
	"context"

	"github.com/jacktea/xgfs/pkg/blob"
)

// RefTracker keeps shard reference counts in sync with metadata.
type RefTracker struct {
	Store Store
	Blob  blob.Store
}

// Add increments refcounts for shard IDs.
func (r *RefTracker) Add(ctx context.Context, shardID string, delta int) error {
	if r == nil || shardID == "" || delta == 0 {
		return nil
	}
	_, err := r.Store.IncRef(ctx, shardID, delta)
	return err
}

// Release decrements refcount and deletes shards no longer in use.
func (r *RefTracker) Release(ctx context.Context, shardID string) error {
	if r == nil || shardID == "" {
		return nil
	}
	refs, err := r.Store.IncRef(ctx, shardID, -1)
	if err != nil {
		return err
	}
	if refs == 0 {
		if err := r.Store.DecideGC(ctx, shardID, refs); err != nil {
			return err
		}
	}
	return nil
}

// ReleaseMany releases multiple shards.
func (r *RefTracker) ReleaseMany(ctx context.Context, shardIDs []string) error {
	for _, id := range shardIDs {
		if err := r.Release(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

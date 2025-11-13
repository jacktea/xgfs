package gc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/meta"
)

// Options configures a Sweeper.
type Options struct {
	Store     meta.Store
	Blob      blob.Store
	BatchSize int
	Logger    func(format string, args ...any)
}

// Sweeper removes zero-ref shards from blob stores.
type Sweeper struct {
	store     meta.Store
	blob      blob.Store
	batchSize int
	logf      func(string, ...any)
}

// NewSweeper wires metadata and blob stores for garbage collection.
func NewSweeper(opts Options) *Sweeper {
	logf := opts.Logger
	if logf == nil {
		logf = log.Printf
	}
	return &Sweeper{
		store:     opts.Store,
		blob:      opts.Blob,
		batchSize: opts.BatchSize,
		logf:      logf,
	}
}

// Sweep performs a best-effort GC pass, returning shards deleted.
func (s *Sweeper) Sweep(ctx context.Context) (int, error) {
	if s.store == nil || s.blob == nil {
		return 0, fmt.Errorf("gc sweeper missing dependencies")
	}
	limit := s.batchSize
	if limit <= 0 {
		limit = 128
	}
	var total int
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		shards, err := s.store.ListZeroRef(ctx, limit)
		if err != nil {
			return total, err
		}
		if len(shards) == 0 {
			return total, nil
		}
		for _, shardID := range shards {
			if err := s.removeShard(ctx, shardID); err != nil {
				return total, err
			}
			total++
		}
		if len(shards) < limit {
			return total, nil
		}
	}
}

// Start launches a background sweep loop until ctx is canceled.
func (s *Sweeper) Start(ctx context.Context, interval time.Duration) context.CancelFunc {
	if interval <= 0 {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			_, err := s.Sweep(ctx)
			if err != nil && !errors.Is(err, context.Canceled) && s.logf != nil {
				s.logf("gc sweep: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return cancel
}

func (s *Sweeper) removeShard(ctx context.Context, shardID string) error {
	if err := s.blob.Delete(ctx, blob.ID(shardID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.store.MarkGCComplete(ctx, shardID)
}

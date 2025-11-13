package meta

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jacktea/xgfs/pkg/fs"
)

// MigrateToBolt copies metadata from an existing Store into a Bolt-backed store.
// The caller is responsible for closing the returned store.
func MigrateToBolt(ctx context.Context, src Store, cfg BoltConfig) (*BoltStore, error) {
	dst, err := NewBoltStore(cfg)
	if err != nil {
		return nil, err
	}
	visited := make(map[fs.ID]struct{})
	queue := []fs.ID{}
	root, err := src.Root(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: fetch root: %w", err)
	}
	queue = append(queue, root.ID)
	maxID := uint64(1)
	shardRefs := make(map[string]int)

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		id := queue[0]
		queue = queue[1:]
		if _, seen := visited[id]; seen {
			continue
		}
		inode, err := src.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("migrate: get %s: %w", id, err)
		}
		if err := dst.Put(ctx, inode); err != nil {
			return nil, fmt.Errorf("migrate: write %s: %w", id, err)
		}
		visited[id] = struct{}{}
		if n, ok := parseInodeNumber(id); ok && n > maxID {
			maxID = n
		}
		for _, shard := range inode.Shards {
			shardRefs[shard.ShardID]++
		}
		children, err := src.Children(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("migrate: list children of %s: %w", id, err)
		}
		for _, child := range children {
			queue = append(queue, child.ID)
		}
	}

	if err := dst.resetShardRefs(shardRefs); err != nil {
		return nil, err
	}
	if err := dst.setNextID(maxID + 1); err != nil {
		return nil, err
	}
	return dst, nil
}

func parseInodeNumber(id fs.ID) (uint64, bool) {
	const prefix = "inode-"
	s := string(id)
	if !strings.HasPrefix(s, prefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(s[len(prefix):], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

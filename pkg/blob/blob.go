package blob

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"
)

// ID is the logical identifier for a shard.
type ID string

// Store is the minimal interface required by higher layers.
type Store interface {
	Put(ctx context.Context, r io.Reader, size int64, opts PutOptions) (ID, int64, error)
	Get(ctx context.Context, id ID) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, id ID) error
	Exists(ctx context.Context, id ID) (bool, error)
}

// PutOptions controls blob persistence.
type PutOptions struct {
	Encrypt   bool
	Key       []byte
	Checksum  string
	DedupOnly bool
}

// Hasher returns a helper for deterministic shard IDs.
func Hasher() hash.Hash {
	return sha256.New()
}

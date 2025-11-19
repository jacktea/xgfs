package meta

import (
	"context"
	"time"

	"github.com/jacktea/xgfs/pkg/encryption"
	"github.com/jacktea/xgfs/pkg/fs"
)

// Type represents inode types.
type Type int

const (
	TypeUnknown Type = iota
	TypeFile
	TypeDirectory
	TypeSymlink
)

// RootID is the canonical ID for the filesystem root.
const RootID fs.ID = 1

// Inode describes a filesystem node with refcounts.
type Inode struct {
	ID        fs.ID
	Parent    fs.ID                         // canonical parent
	Name      string                        // canonical name
	Parents   map[fs.ID]map[string]struct{} // parent -> set of names
	Type      Type
	Size      int64
	Mode      uint32
	UID       uint32
	GID       uint32
	MTime     time.Time
	CTime     time.Time
	LinkCount int
	Shards    []ShardRef
	Metadata  map[string]string
	Target    string // for symlinks
}

// ShardRef ties an inode to stored data.
type ShardRef struct {
	ShardID    string
	Size       int64
	StoredSize int64 `json:"stored_size,omitempty"`
	Offset     int64
	Version    int
	Checksum   string
	Encryption encryption.Method `json:"encryption,omitempty"`
	Encrypted  bool              `json:"encrypted,omitempty"`
}

// Store persists inode metadata.
type Store interface {
	Root(ctx context.Context) (Inode, error)
	Get(ctx context.Context, id fs.ID) (Inode, error)
	Put(ctx context.Context, inode Inode) error
	Delete(ctx context.Context, id fs.ID) error
	Children(ctx context.Context, parent fs.ID) ([]Inode, error)
	AllocateID(ctx context.Context) (fs.ID, error)
	IncRef(ctx context.Context, shardID string, delta int) (int, error)
	DecideGC(ctx context.Context, shardID string, refs int) error
	ListZeroRef(ctx context.Context, limit int) ([]string, error)
	MarkGCComplete(ctx context.Context, shardID string) error

	Begin(ctx context.Context) (Txn, error)
}

// Txn enables atomic metadata updates.
type Txn interface {
	Store
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

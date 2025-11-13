package meta

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	ShardID   string
	Size      int64
	Offset    int64
	Version   int
	Checksum  string
	Encrypted bool
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

// MemoryStore is a simple in-memory implementation for tests.
type MemoryStore struct {
	mu        sync.RWMutex
	inodes    map[fs.ID]Inode
	shards    map[string]int
	pendingGC map[string]struct{}
	nextID    int64
}

// NewMemoryStore creates an empty metadata store.
func NewMemoryStore() *MemoryStore {
	m := &MemoryStore{
		inodes:    make(map[fs.ID]Inode),
		shards:    make(map[string]int),
		pendingGC: make(map[string]struct{}),
		nextID:    1,
	}
	now := time.Now()
	root := Inode{
		ID:        fs.ID("root"),
		Parent:    "",
		Name:      "/",
		Parents:   map[fs.ID]map[string]struct{}{},
		Type:      TypeDirectory,
		Mode:      0o755,
		UID:       0,
		GID:       0,
		MTime:     now,
		CTime:     now,
		LinkCount: 1,
		Metadata:  map[string]string{},
	}
	m.inodes[root.ID] = root
	return m
}

func (m *MemoryStore) Root(ctx context.Context) (Inode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inode, ok := m.inodes["root"]
	if ok {
		return inode, nil
	}
	return Inode{}, fs.ErrNotFound
}

func (m *MemoryStore) Get(ctx context.Context, id fs.ID) (Inode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inode, ok := m.inodes[id]
	if !ok {
		return Inode{}, fs.ErrNotFound
	}
	return inode, nil
}

func (m *MemoryStore) Put(ctx context.Context, inode Inode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inodes[inode.ID] = inode
	return nil
}

func (m *MemoryStore) Delete(ctx context.Context, id fs.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inodes, id)
	return nil
}

func (m *MemoryStore) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Inode
	for _, inode := range m.inodes {
		if inode.Parents == nil {
			continue
		}
		if names, ok := inode.Parents[parent]; ok {
			for name := range names {
				cloned := inode
				cloned.Parent = parent
				cloned.Name = name
				out = append(out, cloned)
			}
		}
	}
	return out, nil
}

func (m *MemoryStore) AllocateID(ctx context.Context) (fs.ID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := fs.ID(fmt.Sprintf("inode-%d", m.nextID))
	m.nextID++
	return id, nil
}

func (m *MemoryStore) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[shardID] += delta
	if m.shards[shardID] <= 0 {
		delete(m.shards, shardID)
		return 0, nil
	}
	if m.pendingGC != nil {
		delete(m.pendingGC, shardID)
	}
	return m.shards[shardID], nil
}

func (m *MemoryStore) DecideGC(ctx context.Context, shardID string, refs int) error {
	if refs > 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingGC == nil {
		m.pendingGC = make(map[string]struct{})
	}
	m.pendingGC[shardID] = struct{}{}
	return nil
}

func (m *MemoryStore) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for shardID := range m.pendingGC {
		out = append(out, shardID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *MemoryStore) MarkGCComplete(ctx context.Context, shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingGC, shardID)
	return nil
}

func (m *MemoryStore) Begin(ctx context.Context) (Txn, error) {
	return &memTxn{store: m}, nil
}

type memTxn struct {
	store *MemoryStore
}

func (t *memTxn) Root(ctx context.Context) (Inode, error)          { return t.store.Root(ctx) }
func (t *memTxn) Get(ctx context.Context, id fs.ID) (Inode, error) { return t.store.Get(ctx, id) }
func (t *memTxn) Put(ctx context.Context, inode Inode) error       { return t.store.Put(ctx, inode) }
func (t *memTxn) Delete(ctx context.Context, id fs.ID) error       { return t.store.Delete(ctx, id) }
func (t *memTxn) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	return t.store.Children(ctx, parent)
}
func (t *memTxn) AllocateID(ctx context.Context) (fs.ID, error) { return t.store.AllocateID(ctx) }
func (t *memTxn) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	return t.store.IncRef(ctx, shardID, delta)
}
func (t *memTxn) DecideGC(ctx context.Context, shardID string, refs int) error {
	return t.store.DecideGC(ctx, shardID, refs)
}
func (t *memTxn) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	return t.store.ListZeroRef(ctx, limit)
}
func (t *memTxn) MarkGCComplete(ctx context.Context, shardID string) error {
	return t.store.MarkGCComplete(ctx, shardID)
}
func (t *memTxn) Begin(ctx context.Context) (Txn, error) { return t.store.Begin(ctx) }
func (t *memTxn) Commit(ctx context.Context) error       { return nil }
func (t *memTxn) Rollback(ctx context.Context) error     { return nil }

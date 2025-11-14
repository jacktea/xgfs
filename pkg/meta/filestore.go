package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
)

// FileStore persists metadata on disk using a JSON snapshot.
type FileStore struct {
	mu        sync.RWMutex
	path      string
	inodes    map[fs.ID]Inode
	shards    map[string]int
	pendingGC map[string]struct{}
	nextID    int64
}

type fileState struct {
	Inodes    map[fs.ID]Inode `json:"inodes"`
	Shards    map[string]int  `json:"shards"`
	PendingGC []string        `json:"pending_gc"`
	NextID    int64           `json:"next_id"`
}

// NewFileStore creates or loads a FileStore snapshot at path.
func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("filestore mkdir: %w", err)
	}
	fs := &FileStore{path: path}
	if err := fs.loadOrInit(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (f *FileStore) loadOrInit() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := os.Stat(f.path); err == nil {
		data, err := os.ReadFile(f.path)
		if err != nil {
			return err
		}
		var state fileState
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
		f.inodes = state.Inodes
		f.shards = state.Shards
		f.pendingGC = make(map[string]struct{}, len(state.PendingGC))
		for _, shardID := range state.PendingGC {
			f.pendingGC[shardID] = struct{}{}
		}
		if f.pendingGC == nil {
			f.pendingGC = make(map[string]struct{})
		}
		if state.NextID < 2 {
			state.NextID = 2
		}
		f.nextID = state.NextID
		return nil
	}
	now := time.Now()
	root := Inode{
		ID:        RootID,
		Parent:    0,
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
	f.inodes = map[fs.ID]Inode{root.ID: root}
	f.shards = make(map[string]int)
	f.pendingGC = make(map[string]struct{})
	f.nextID = 2
	return f.persistLocked()
}

func (f *FileStore) Root(ctx context.Context) (Inode, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	inode, ok := f.inodes[RootID]
	if !ok {
		return Inode{}, fs.ErrNotFound
	}
	return inode, nil
}

func (f *FileStore) Get(ctx context.Context, id fs.ID) (Inode, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	inode, ok := f.inodes[id]
	if !ok {
		return Inode{}, fs.ErrNotFound
	}
	return inode, nil
}

func (f *FileStore) Put(ctx context.Context, inode Inode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inodes[inode.ID] = inode
	return f.persistLocked()
}

func (f *FileStore) Delete(ctx context.Context, id fs.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.inodes, id)
	return f.persistLocked()
}

func (f *FileStore) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []Inode
	for _, inode := range f.inodes {
		if inode.Parents == nil {
			continue
		}
		if names, ok := inode.Parents[parent]; ok {
			for name := range names {
				clone := inode
				clone.Parent = parent
				clone.Name = name
				out = append(out, clone)
			}
		}
	}
	return out, nil
}

func (f *FileStore) AllocateID(ctx context.Context) (fs.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := fs.ID(f.nextID)
	f.nextID++
	if err := f.persistLocked(); err != nil {
		return 0, err
	}
	return id, nil
}

func (f *FileStore) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shards[shardID] += delta
	if f.shards[shardID] <= 0 {
		delete(f.shards, shardID)
		return 0, f.persistLocked()
	}
	if f.pendingGC != nil {
		delete(f.pendingGC, shardID)
	}
	if err := f.persistLocked(); err != nil {
		return 0, err
	}
	return f.shards[shardID], nil
}

func (f *FileStore) DecideGC(ctx context.Context, shardID string, refs int) error {
	if refs > 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingGC == nil {
		f.pendingGC = make(map[string]struct{})
	}
	f.pendingGC[shardID] = struct{}{}
	return f.persistLocked()
}

func (f *FileStore) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []string
	for shardID := range f.pendingGC {
		out = append(out, shardID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *FileStore) MarkGCComplete(ctx context.Context, shardID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pendingGC, shardID)
	return f.persistLocked()
}

func (f *FileStore) Begin(ctx context.Context) (Txn, error) {
	return &fileTxn{store: f}, nil
}

type fileTxn struct {
	store *FileStore
}

func (t *fileTxn) Root(ctx context.Context) (Inode, error)          { return t.store.Root(ctx) }
func (t *fileTxn) Get(ctx context.Context, id fs.ID) (Inode, error) { return t.store.Get(ctx, id) }
func (t *fileTxn) Put(ctx context.Context, inode Inode) error       { return t.store.Put(ctx, inode) }
func (t *fileTxn) Delete(ctx context.Context, id fs.ID) error       { return t.store.Delete(ctx, id) }
func (t *fileTxn) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	return t.store.Children(ctx, parent)
}
func (t *fileTxn) AllocateID(ctx context.Context) (fs.ID, error) { return t.store.AllocateID(ctx) }
func (t *fileTxn) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	return t.store.IncRef(ctx, shardID, delta)
}
func (t *fileTxn) DecideGC(ctx context.Context, shardID string, refs int) error {
	return t.store.DecideGC(ctx, shardID, refs)
}
func (t *fileTxn) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	return t.store.ListZeroRef(ctx, limit)
}
func (t *fileTxn) MarkGCComplete(ctx context.Context, shardID string) error {
	return t.store.MarkGCComplete(ctx, shardID)
}
func (t *fileTxn) Begin(ctx context.Context) (Txn, error) { return t.store.Begin(ctx) }
func (t *fileTxn) Commit(ctx context.Context) error       { return nil }
func (t *fileTxn) Rollback(ctx context.Context) error     { return nil }

func (f *FileStore) persistLocked() error {
	state := fileState{
		Inodes:    f.inodes,
		Shards:    f.shards,
		PendingGC: make([]string, 0, len(f.pendingGC)),
		NextID:    f.nextID,
	}
	for shardID := range f.pendingGC {
		state.PendingGC = append(state.PendingGC, shardID)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), "meta-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), f.path); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}

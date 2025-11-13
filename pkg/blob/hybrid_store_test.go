package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
	"testing"
)

type memoryStore struct {
	mu   sync.Mutex
	data map[ID][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[ID][]byte)}
}

func (m *memoryStore) Put(ctx context.Context, r io.Reader, size int64, opts PutOptions) (ID, int64, error) {
	buf, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(buf)
	id := ID(hex.EncodeToString(sum[:]))
	m.mu.Lock()
	m.data[id] = append([]byte(nil), buf...)
	m.mu.Unlock()
	return id, int64(len(buf)), nil
}

func (m *memoryStore) Get(ctx context.Context, id ID) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[id]
	if !ok {
		return nil, 0, io.EOF
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), int64(len(data)), nil
}

func (m *memoryStore) Delete(ctx context.Context, id ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, id)
	return nil
}

func (m *memoryStore) Exists(ctx context.Context, id ID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[id]
	return ok, nil
}

func TestHybridStoreMirrorsWrites(t *testing.T) {
	ctx := context.Background()
	primary := newMemoryStore()
	secondary := newMemoryStore()
	hybrid, err := NewHybridStore(primary, secondary, HybridOptions{MirrorSecondary: true})
	if err != nil {
		t.Fatalf("new hybrid: %v", err)
	}
	id, _, err := hybrid.Put(ctx, bytes.NewReader([]byte("hello")), 5, PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if ok, _ := primary.Exists(ctx, id); !ok {
		t.Fatalf("primary missing shard")
	}
	if ok, _ := secondary.Exists(ctx, id); !ok {
		t.Fatalf("secondary missing shard")
	}
}

func TestHybridStoreCachesOnRead(t *testing.T) {
	ctx := context.Background()
	primary := newMemoryStore()
	secondary := newMemoryStore()
	hybrid, err := NewHybridStore(primary, secondary, HybridOptions{MirrorSecondary: true, CacheOnRead: true})
	if err != nil {
		t.Fatalf("new hybrid: %v", err)
	}
	id, _, err := hybrid.Put(ctx, bytes.NewReader([]byte("payload")), 7, PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	// Simulate primary loss.
	primary.Delete(ctx, id)
	r, _, err := hybrid.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(r)
	r.Close()
	if string(body) != "payload" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if ok, _ := primary.Exists(ctx, id); !ok {
		t.Fatalf("expected cache refill in primary")
	}
}

func TestHybridStoreDeleteRemovesBothStores(t *testing.T) {
	ctx := context.Background()
	primary := newMemoryStore()
	secondary := newMemoryStore()
	hybrid, err := NewHybridStore(primary, secondary, HybridOptions{MirrorSecondary: true})
	if err != nil {
		t.Fatalf("new hybrid: %v", err)
	}
	id, _, err := hybrid.Put(ctx, bytes.NewReader([]byte("gc-data")), 7, PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := hybrid.Delete(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if ok, _ := primary.Exists(ctx, id); ok {
		t.Fatalf("primary still has data after delete")
	}
	if ok, _ := secondary.Exists(ctx, id); ok {
		t.Fatalf("secondary still has data after delete")
	}
}

package meta

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/jacktea/xgfs/pkg/fs"
)

var (
	bucketMeta    = []byte("meta")
	bucketInodes  = []byte("inodes")
	bucketShards  = []byte("shards")
	bucketGCQueue = []byte("gc_queue")

	metaNextIDKey = []byte("next-id")
	rootInodeID   = RootID
)

// BoltConfig configures the BoltDB-backed store.
type BoltConfig struct {
	Path         string
	BucketPrefix string
	NoSync       bool
	Timeout      time.Duration
}

// BoltStore persists metadata in BoltDB.
type BoltStore struct {
	cfg BoltConfig
	db  *bolt.DB
}

// NewBoltStore initialises a Bolt-backed metadata store.
func NewBoltStore(cfg BoltConfig) (*BoltStore, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("boltdb: path is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 1 * time.Second
	}
	opts := bolt.Options{
		Timeout: cfg.Timeout,
		NoSync:  cfg.NoSync,
	}
	db, err := bolt.Open(cfg.Path, 0o600, &opts)
	if err != nil {
		return nil, fmt.Errorf("boltdb: open: %w", err)
	}
	store := &BoltStore{cfg: cfg, db: db}
	if err := store.init(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (b *BoltStore) init() error {
	return b.db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{bucketMeta, bucketInodes, bucketShards, bucketGCQueue} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return fmt.Errorf("boltdb: create bucket %s: %w", bucket, err)
			}
		}
		meta := tx.Bucket(bucketMeta)
		if meta.Get(metaNextIDKey) == nil {
			if err := meta.Put(metaNextIDKey, encodeUint64(2)); err != nil {
				return err
			}
		} else if cur := decodeUint64(meta.Get(metaNextIDKey)); cur < 2 {
			if err := meta.Put(metaNextIDKey, encodeUint64(2)); err != nil {
				return err
			}
		}
		inodes := tx.Bucket(bucketInodes)
		if inodes.Get(idKey(rootInodeID)) == nil {
			now := time.Now()
			root := Inode{
				ID:        rootInodeID,
				Parent:    0,
				Name:      "/",
				Type:      TypeDirectory,
				Mode:      0o755,
				UID:       0,
				GID:       0,
				MTime:     now,
				CTime:     now,
				LinkCount: 1,
				Metadata:  map[string]string{},
				Parents:   map[fs.ID]map[string]struct{}{},
			}
			data, err := json.Marshal(root)
			if err != nil {
				return err
			}
			if err := inodes.Put(idKey(root.ID), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltStore) Root(ctx context.Context) (Inode, error) {
	var inode Inode
	err := b.db.View(func(tx *bolt.Tx) error {
		var err error
		inode, err = b.getInode(tx, rootInodeID)
		return err
	})
	return inode, err
}

func (b *BoltStore) Get(ctx context.Context, id fs.ID) (Inode, error) {
	var inode Inode
	err := b.db.View(func(tx *bolt.Tx) error {
		var err error
		inode, err = b.getInode(tx, id)
		return err
	})
	return inode, err
}

func (b *BoltStore) Put(ctx context.Context, inode Inode) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(inode)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketInodes).Put(idKey(inode.ID), data)
	})
}

func (b *BoltStore) Delete(ctx context.Context, id fs.ID) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInodes).Delete(idKey(id))
	})
}

func (b *BoltStore) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	var out []Inode
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketInodes)
		return bkt.ForEach(func(k, v []byte) error {
			inode, err := decodeInode(v)
			if err != nil {
				return err
			}
			if inode.Parents != nil {
				if names, ok := inode.Parents[parent]; ok {
					for name := range names {
						child := inode
						child.Parent = parent
						child.Name = name
						out = append(out, child)
					}
				}
			}
			return nil
		})
	})
	return out, err
}

func (b *BoltStore) AllocateID(ctx context.Context) (fs.ID, error) {
	var id fs.ID
	err := b.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		cur := decodeUint64(meta.Get(metaNextIDKey))
		next := cur + 1
		if err := meta.Put(metaNextIDKey, encodeUint64(next)); err != nil {
			return err
		}
		id = fs.ID(cur)
		return nil
	})
	return id, err
}

func (b *BoltStore) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	var refs int
	err := b.db.Update(func(tx *bolt.Tx) error {
		refsBucket := tx.Bucket(bucketShards)
		key := []byte(shardID)
		cur := decodeInt(refsBucket.Get(key))
		cur += delta
		if cur <= 0 {
			if err := refsBucket.Delete(key); err != nil {
				return err
			}
			refs = 0
			return nil
		}
		if err := refsBucket.Put(key, encodeInt(cur)); err != nil {
			return err
		}
		refs = cur
		if cur > 0 {
			tx.Bucket(bucketGCQueue).Delete(key)
		}
		return nil
	})
	return refs, err
}

func (b *BoltStore) DecideGC(ctx context.Context, shardID string, refs int) error {
	if refs > 0 {
		return nil
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketGCQueue).Put([]byte(shardID), []byte{})
	})
}

func (b *BoltStore) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	var out []string
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketGCQueue).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			out = append(out, string(k))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func (b *BoltStore) MarkGCComplete(ctx context.Context, shardID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketGCQueue).Delete([]byte(shardID))
	})
}

func (b *BoltStore) Begin(ctx context.Context) (Txn, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &boltTxn{store: b, tx: tx}, nil
}

// Close releases the underlying BoltDB.
func (b *BoltStore) Close() error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

func (b *BoltStore) getInode(tx *bolt.Tx, id fs.ID) (Inode, error) {
	data := tx.Bucket(bucketInodes).Get(idKey(id))
	if data == nil {
		return Inode{}, fs.ErrNotFound
	}
	return decodeInode(data)
}

func decodeInode(data []byte) (Inode, error) {
	var inode Inode
	if err := json.Unmarshal(data, &inode); err != nil {
		return Inode{}, err
	}
	return inode, nil
}

func idKey(id fs.ID) []byte {
	return encodeUint64(uint64(id))
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func encodeInt(v int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(int64(v)))
	return buf
}

func decodeInt(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return int(int64(binary.BigEndian.Uint64(b)))
}

type boltTxn struct {
	store  *BoltStore
	tx     *bolt.Tx
	closed bool
}

func (t *boltTxn) Root(ctx context.Context) (Inode, error) {
	return t.store.getInode(t.tx, rootInodeID)
}

func (t *boltTxn) Get(ctx context.Context, id fs.ID) (Inode, error) {
	return t.store.getInode(t.tx, id)
}

func (t *boltTxn) Put(ctx context.Context, inode Inode) error {
	data, err := json.Marshal(inode)
	if err != nil {
		return err
	}
	return t.tx.Bucket(bucketInodes).Put(idKey(inode.ID), data)
}

func (t *boltTxn) Delete(ctx context.Context, id fs.ID) error {
	return t.tx.Bucket(bucketInodes).Delete(idKey(id))
}

func (t *boltTxn) Children(ctx context.Context, parent fs.ID) ([]Inode, error) {
	var out []Inode
	bkt := t.tx.Bucket(bucketInodes)
	if err := bkt.ForEach(func(k, v []byte) error {
		inode, err := decodeInode(v)
		if err != nil {
			return err
		}
		if inode.Parents != nil {
			if names, ok := inode.Parents[parent]; ok {
				for name := range names {
					child := inode
					child.Parent = parent
					child.Name = name
					out = append(out, child)
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (t *boltTxn) AllocateID(ctx context.Context) (fs.ID, error) {
	meta := t.tx.Bucket(bucketMeta)
	cur := decodeUint64(meta.Get(metaNextIDKey))
	next := cur + 1
	if err := meta.Put(metaNextIDKey, encodeUint64(next)); err != nil {
		return 0, err
	}
	return fs.ID(cur), nil
}

func (t *boltTxn) IncRef(ctx context.Context, shardID string, delta int) (int, error) {
	bkt := t.tx.Bucket(bucketShards)
	key := []byte(shardID)
	cur := decodeInt(bkt.Get(key))
	cur += delta
	if cur <= 0 {
		if err := bkt.Delete(key); err != nil {
			return 0, err
		}
		t.tx.Bucket(bucketGCQueue).Delete(key)
		return 0, nil
	}
	if err := bkt.Put(key, encodeInt(cur)); err != nil {
		return 0, err
	}
	t.tx.Bucket(bucketGCQueue).Delete(key)
	return cur, nil
}

func (t *boltTxn) DecideGC(ctx context.Context, shardID string, refs int) error {
	if refs > 0 {
		return nil
	}
	return t.tx.Bucket(bucketGCQueue).Put([]byte(shardID), []byte{})
}

func (t *boltTxn) ListZeroRef(ctx context.Context, limit int) ([]string, error) {
	var out []string
	c := t.tx.Bucket(bucketGCQueue).Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		out = append(out, string(k))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (t *boltTxn) MarkGCComplete(ctx context.Context, shardID string) error {
	return t.tx.Bucket(bucketGCQueue).Delete([]byte(shardID))
}

func (t *boltTxn) Begin(ctx context.Context) (Txn, error) {
	return nil, errors.New("boltdb: nested transactions not supported")
}

func (t *boltTxn) Commit(ctx context.Context) error {
	if t.closed {
		return errors.New("boltdb: transaction already closed")
	}
	t.closed = true
	return t.tx.Commit()
}

func (t *boltTxn) Rollback(ctx context.Context) error {
	if t.closed {
		return nil
	}
	t.closed = true
	return t.tx.Rollback()
}

func (b *BoltStore) setNextID(next uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(metaNextIDKey, encodeUint64(next))
	})
}

func (b *BoltStore) resetShardRefs(refs map[string]int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		shards := tx.Bucket(bucketShards)
		if err := shards.ForEach(func(k, _ []byte) error {
			return shards.Delete(k)
		}); err != nil {
			return err
		}
		gc := tx.Bucket(bucketGCQueue)
		if err := gc.ForEach(func(k, _ []byte) error {
			return gc.Delete(k)
		}); err != nil {
			return err
		}
		for id, count := range refs {
			if count <= 0 {
				continue
			}
			if err := shards.Put([]byte(id), encodeInt(count)); err != nil {
				return err
			}
		}
		return nil
	})
}

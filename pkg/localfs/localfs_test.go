package localfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/vfs"
)

func TestLocalFsCreateAndRead(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/foo.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := "hello sharded world"
	if _, err := obj.Write(ctx, bytes.NewBufferString(payload), ioOpts()); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := readObject(ctx, obj)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != payload {
		t.Fatalf("expected %q got %q", payload, got)
	}
}

func TestLocalFsHardLinkSharesData(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/alpha.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("alpha"), ioOpts()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := backend.Link(ctx, "/alpha.txt", "/beta.txt", fs.LinkHard); err != nil {
		t.Fatalf("link: %v", err)
	}
	beta, err := backend.Stat(ctx, "/beta.txt")
	if err != nil {
		t.Fatalf("stat beta: %v", err)
	}
	if _, err := beta.Write(ctx, bytes.NewBufferString("beta"), ioOpts()); err != nil {
		t.Fatalf("write beta: %v", err)
	}
	gotAlpha, err := readObject(ctx, obj)
	if err != nil {
		t.Fatalf("read alpha: %v", err)
	}
	if gotAlpha != "beta" {
		t.Fatalf("expected data to reflect last write, got %q", gotAlpha)
	}
}

func TestLocalFsCopyProvidesCopyOnWrite(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	src, err := backend.Create(ctx, "/copy-src.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create src: %v", err)
	}
	if _, err := src.Write(ctx, bytes.NewBufferString("alpha"), ioOpts()); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if _, err := backend.Copy(ctx, "/copy-src.txt", "/copy-dst.txt", fs.CopyOptions{}); err != nil {
		t.Fatalf("copy: %v", err)
	}
	dst, err := backend.Stat(ctx, "/copy-dst.txt")
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	gotDst, err := readObject(ctx, dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if gotDst != "alpha" {
		t.Fatalf("expected dst to match src, got %q", gotDst)
	}
	if _, err := dst.Write(ctx, bytes.NewBufferString("beta"), ioOpts()); err != nil {
		t.Fatalf("write dst: %v", err)
	}
	gotSrc, err := readObject(ctx, src)
	if err != nil {
		t.Fatalf("read src: %v", err)
	}
	if gotSrc != "alpha" {
		t.Fatalf("expected src unaffected, got %q", gotSrc)
	}
	gotDst, err = readObject(ctx, dst)
	if err != nil {
		t.Fatalf("read dst after write: %v", err)
	}
	if gotDst != "beta" {
		t.Fatalf("expected dst updated data, got %q", gotDst)
	}
}

func TestLocalFsConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/concurrent.bin", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := bytes.Repeat([]byte("data-block-"), 1<<12)
	if _, err := obj.Write(ctx, bytes.NewReader(payload), fs.IOOptions{Concurrency: 4}); err != nil {
		t.Fatalf("write: %v", err)
	}
	writer := newSliceWriter(len(payload))
	if _, err := obj.Read(ctx, writer, fs.IOOptions{Concurrency: 4}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestLocalFsReadAt(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/segments.bin", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := []byte("abcdefghijklmnopqrstuvwxyz")
	if _, err := obj.Write(ctx, bytes.NewReader(payload), fs.IOOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 6)
	n, err := obj.ReadAt(ctx, buf, 5, fs.IOOptions{})
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("expected %d bytes got %d", len(buf), n)
	}
	if got := string(buf); got != string(payload[5:11]) {
		t.Fatalf("unexpected data %q", got)
	}
	tail := make([]byte, 10)
	n, err = obj.ReadAt(ctx, tail, int64(len(payload)-4), fs.IOOptions{})
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if got := string(tail[:n]); got != string(payload[len(payload)-4:]) {
		t.Fatalf("tail mismatch %q", got)
	}
}

func TestLocalFsLockUnlock(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	user := vfs.User{UID: 1000, GID: 1000}
	posix := backend.PosixAdapter()
	if err := posix.Lock(ctx, "/foo", vfs.LockOptions{Exclusive: true}, user); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := posix.Lock(ctx, "/foo", vfs.LockOptions{}, user); err != nil {
		t.Fatalf("reentrant lock: %v", err)
	}
	if err := posix.Unlock(ctx, "/foo", vfs.LockOptions{}, user); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := posix.Unlock(ctx, "/foo", vfs.LockOptions{}, user); err != nil {
		t.Fatalf("unlock final: %v", err)
	}
	if err := posix.Unlock(ctx, "/foo", vfs.LockOptions{}, user); err == nil {
		t.Fatalf("expected error unlocking free lock")
	}
}

func TestLocalFsRenameDirectory(t *testing.T) {
	ctx := context.Background()
	backend, err := New(ctx, Config{BlobRoot: t.TempDir(), CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	if _, err := backend.Mkdir(ctx, "/dir", fs.MkdirOptions{Mode: 0o755}); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	if _, err := backend.Mkdir(ctx, "/dir/sub", fs.MkdirOptions{Mode: 0o755, Parents: true}); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	obj, err := backend.Create(ctx, "/dir/sub/file.txt", fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("data"), fs.IOOptions{}); err != nil {
		t.Fatalf("write file: %v", err)
	}
	posix := backend.PosixAdapter()
	rootInode, err := backend.store.Root(ctx)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	beforeDir, err := backend.child(ctx, rootInode.ID, "dir")
	if err != nil {
		t.Fatalf("child dir: %v", err)
	}
	t.Logf("before parents: %+v", beforeDir.Parents)
	if err := posix.Rename(ctx, "/dir", "/dir2", vfs.RenameOptions{}, vfs.User{}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	afterDir, err := backend.child(ctx, rootInode.ID, "dir2")
	if err != nil {
		t.Fatalf("child dir2: %v", err)
	}
	t.Logf("after parents: %+v", afterDir.Parents)
	if _, err := backend.Stat(ctx, "/dir2/sub/file.txt"); err != nil {
		t.Fatalf("stat new file: %v", err)
	}
	if entry, err := backend.child(ctx, rootInode.ID, "dir"); err == nil {
		t.Fatalf("expected backend child dir missing, parents=%+v", entry.Parents)
	}
	if _, err := backend.Stat(ctx, "/dir/sub/file.txt"); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected old file missing, got %v", err)
	}
	rootEntries, err := backend.List(ctx, "/", fs.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var names []string
	for entry := range rootEntries {
		names = append(names, entry.Name)
	}
	found := false
	for _, name := range names {
		if name == "dir2" {
			found = true
		}
		if name == "dir" {
			t.Fatalf("found old directory name")
		}
	}
	if !found {
		t.Fatalf("dir2 not found in listing: %v", names)
	}
}

func TestLocalFsRunGCClearsZeroRefShards(t *testing.T) {
	ctx := context.Background()
	blobRoot := t.TempDir()
	backend, err := New(ctx, Config{
		BlobRoot:      blobRoot,
		CacheEntries:  16,
		DisableAutoGC: true,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/gc.bin", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := bytes.Repeat([]byte("garbage"), 8)
	if _, err := obj.Write(ctx, bytes.NewReader(payload), fs.IOOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	inode, err := backend.resolve(ctx, "/gc.bin")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(inode.Shards) == 0 {
		t.Fatalf("expected shards for gc.bin")
	}
	shardID := inode.Shards[0].ShardID
	exists, err := backend.blobs.Exists(ctx, blob.ID(shardID))
	if err != nil || !exists {
		t.Fatalf("shard missing before gc, exists=%v err=%v", exists, err)
	}
	if err := backend.Remove(ctx, "/gc.bin", fs.RemoveOptions{}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	pending, err := backend.store.ListZeroRef(ctx, 10)
	if err != nil {
		t.Fatalf("list zero: %v", err)
	}
	if len(pending) == 0 {
		t.Fatalf("expected pending shard after delete")
	}
	count, err := backend.RunGC(ctx)
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 shard deleted, got %d", count)
	}
	exists, err = backend.blobs.Exists(ctx, blob.ID(shardID))
	if err != nil {
		t.Fatalf("blob exists after gc: %v", err)
	}
	if exists {
		t.Fatalf("expected shard deleted after gc")
	}
}

func readObject(ctx context.Context, obj fs.Object) (string, error) {
	buf := bytes.NewBuffer(nil)
	writer := &writerAt{Buffer: buf}
	if _, err := obj.Read(ctx, writer, ioOpts()); err != nil {
		return "", err
	}
	return buf.String(), nil
}

type writerAt struct {
	*bytes.Buffer
	pos int64
}

func (w *writerAt) WriteAt(p []byte, off int64) (int, error) {
	if off != w.pos {
		return 0, fmt.Errorf("expected offset %d got %d", w.pos, off)
	}
	n, err := w.Buffer.Write(p)
	w.pos += int64(n)
	return n, err
}

func fsCreateOpts() fs.CreateOptions {
	return fs.CreateOptions{Mode: 0o644, Overwrite: true}
}

func ioOpts() fs.IOOptions { return fs.IOOptions{} }

func TestLocalFsMetadataPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.json")
	blobRoot := filepath.Join(dir, "blobs")
	backend, err := New(ctx, Config{BlobRoot: blobRoot, MetadataPath: metaPath, CacheEntries: 16})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	if _, err := backend.Create(ctx, "/persist.txt", fsCreateOpts()); err != nil {
		t.Fatalf("create: %v", err)
	}
	backend2, err := New(ctx, Config{BlobRoot: blobRoot, MetadataPath: metaPath, CacheEntries: 16})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	if _, err := backend2.Stat(ctx, "/persist.txt"); err != nil {
		t.Fatalf("stat persisted: %v", err)
	}
}

func TestLocalFsHybridMirrorsSecondary(t *testing.T) {
	ctx := context.Background()
	primary, err := blob.NewPathStore(t.TempDir())
	if err != nil {
		t.Fatalf("primary store: %v", err)
	}
	secondary := newTrackingStore()
	backend, err := New(ctx, Config{
		BlobStore:      primary,
		SecondaryStore: secondary,
		HybridOptions: &blob.HybridOptions{
			MirrorSecondary: true,
			CacheOnRead:     true,
		},
		CacheEntries: 16,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/hybrid.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("hybrid-data"), ioOpts()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if secondary.putCount() == 0 {
		t.Fatalf("expected secondary writes via hybrid store")
	}
}

type trackingStore struct {
	mu   sync.Mutex
	data map[blob.ID][]byte
	puts int
}

func newTrackingStore() *trackingStore {
	return &trackingStore{data: make(map[blob.ID][]byte)}
}

func (t *trackingStore) Put(ctx context.Context, r io.Reader, size int64, opts blob.PutOptions) (blob.ID, int64, error) {
	buf, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	id := blob.ID(fmt.Sprintf("tracking-%d", len(buf)))
	t.mu.Lock()
	t.data[id] = append([]byte(nil), buf...)
	t.puts++
	t.mu.Unlock()
	return id, int64(len(buf)), nil
}

func (t *trackingStore) Get(ctx context.Context, id blob.ID) (io.ReadCloser, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	buf, ok := t.data[id]
	if !ok {
		return nil, 0, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(buf)), int64(len(buf)), nil
}

func (t *trackingStore) Delete(ctx context.Context, id blob.ID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.data, id)
	return nil
}

func (t *trackingStore) Exists(ctx context.Context, id blob.ID) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.data[id]
	return ok, nil
}

func (t *trackingStore) putCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.puts
}

type sliceWriter struct {
	buf []byte
}

func newSliceWriter(size int) *sliceWriter {
	return &sliceWriter{buf: make([]byte, size)}
}

func (s *sliceWriter) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	end := int(off) + len(p)
	if end > len(s.buf) {
		expanded := make([]byte, end)
		copy(expanded, s.buf)
		s.buf = expanded
	}
	copy(s.buf[int(off):end], p)
	return len(p), nil
}

func (s *sliceWriter) Bytes() []byte {
	return s.buf
}

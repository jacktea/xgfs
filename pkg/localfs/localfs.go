package localfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/cache"
	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/gc"
	"github.com/jacktea/xgfs/pkg/meta"
	"github.com/jacktea/xgfs/pkg/sharder"
)

// Config contains backend settings.
type Config struct {
	Name             string
	BlobRoot         string
	ChunkSize        int64
	Encrypt          bool
	Key              []byte
	CacheEntries     int
	CacheTTL         time.Duration
	MetaStore        meta.Store
	MetadataPath     string
	BoltMetadataPath string
	BoltOptions      *meta.BoltConfig
	BlobStore        blob.Store
	SecondaryStore   blob.Store
	HybridOptions    *blob.HybridOptions
	DisableAutoGC    bool
	GCInterval       time.Duration
	GCBatchSize      int
}

// LocalFs implements fs.Fs backed by meta + blob stores.
type LocalFs struct {
	cfg          Config
	store        meta.Store
	blobs        blob.Store
	tracker      *meta.RefTracker
	metaCache    *cache.Cache
	features     fs.Features
	writerOpts   sharder.WriterOptions
	cacheEnabled bool
	mu           sync.RWMutex
	gcSweeper    *gc.Sweeper
	gcCancel     context.CancelFunc
	lockMu       sync.Mutex
	locks        map[string]*lockRecord
}

var _ fs.PosixFs = (*LocalFs)(nil)

// New spins up a local filesystem backend.
func New(ctx context.Context, cfg Config) (*LocalFs, error) {
	if cfg.Name == "" {
		cfg.Name = "local"
	}
	if cfg.BlobStore == nil && cfg.BlobRoot == "" {
		cfg.BlobRoot = ".xgfs/blobs"
	}
	if cfg.CacheEntries == 0 {
		cfg.CacheEntries = 1024
	}
	if cfg.Encrypt && len(cfg.Key) != 32 {
		return nil, fmt.Errorf("localfs: encryption key must be 32 bytes")
	}
	if cfg.GCBatchSize <= 0 {
		cfg.GCBatchSize = 128
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 30 * time.Second
	}
	store, err := resolveMetaStore(cfg)
	if err != nil {
		return nil, err
	}
	blobStore, err := resolveBlobStore(cfg)
	if err != nil {
		return nil, err
	}
	tracker := &meta.RefTracker{Store: store, Blob: blobStore}
	metaCache := cache.New(cfg.CacheEntries, cfg.CacheTTL)
	fs := &LocalFs{
		cfg:       cfg,
		store:     store,
		blobs:     blobStore,
		tracker:   tracker,
		metaCache: metaCache,
		features: fs.Features{
			SupportsHardlinks:   true,
			SupportsSymlinks:    true,
			SupportsCopyOnWrite: true,
			SupportsMultipart:   true,
		},
		writerOpts: sharder.WriterOptions{
			ChunkSize: cfg.ChunkSize,
			Encrypt:   cfg.Encrypt,
			Key:       cfg.Key,
		},
		cacheEnabled: cfg.CacheEntries > 0,
		locks:        make(map[string]*lockRecord),
	}
	sweeper := gc.NewSweeper(gc.Options{
		Store:     store,
		Blob:      blobStore,
		BatchSize: cfg.GCBatchSize,
	})
	fs.gcSweeper = sweeper
	if !cfg.DisableAutoGC {
		fs.gcCancel = sweeper.Start(ctx, cfg.GCInterval)
	}
	return fs, nil
}

func init() {
	fs.Register("local", func(ctx context.Context, raw map[string]any) (fs.Fs, error) {
		cfg := Config{}
		if v, ok := raw["name"].(string); ok {
			cfg.Name = v
		}
		if v, ok := raw["blob_root"].(string); ok {
			cfg.BlobRoot = v
		}
		if v, ok := raw["chunk_size"].(int64); ok && v > 0 {
			cfg.ChunkSize = v
		}
		if v, ok := raw["encrypt"].(bool); ok {
			cfg.Encrypt = v
		}
		if v, ok := raw["key"].([]byte); ok {
			cfg.Key = v
		}
		if v, ok := raw["cache_entries"].(int); ok {
			cfg.CacheEntries = v
		}
		if v, ok := raw["cache_ttl"].(time.Duration); ok {
			cfg.CacheTTL = v
		}
		return New(ctx, cfg)
	})
}

func (l *LocalFs) Name() string { return l.cfg.Name }

func (l *LocalFs) Features() fs.Features { return l.features }

func (l *LocalFs) Root(ctx context.Context) (fs.Directory, error) {
	inode, err := l.store.Root(ctx)
	if err != nil {
		return nil, err
	}
	return &localDirectory{fs: l, inode: inode, path: "/"}, nil
}

func (l *LocalFs) Stat(ctx context.Context, p string) (fs.Object, error) {
	inode, err := l.resolve(ctx, p)
	if err != nil {
		return nil, err
	}
	if inode.Type != meta.TypeFile {
		return nil, fs.ErrNotSupported
	}
	return &localObject{fs: l, inode: inode, path: cleanPath(p)}, nil
}

func (l *LocalFs) Create(ctx context.Context, p string, opts fs.CreateOptions) (fs.Object, error) {
	dirPath, fileName := splitParent(p)
	if fileName == "" {
		return nil, fmt.Errorf("missing file name in path %s", p)
	}
	parentDir, err := l.resolveDir(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	inode, err := l.child(ctx, parentDir.inode.ID, fileName)
	if err == nil && !opts.Overwrite {
		return nil, fs.ErrAlreadyExist
	}
	if err == nil && opts.Overwrite {
		obj := &localObject{fs: l, inode: inode, path: cleanPath(p)}
		return obj, nil
	}
	node, err := l.newFileInode(ctx, parentDir.inode.ID, fileName, opts)
	if err != nil {
		return nil, err
	}
	l.cachePut(cleanPath(p), node)
	return &localObject{fs: l, inode: node, path: cleanPath(p)}, nil
}

func (l *LocalFs) Mkdir(ctx context.Context, p string, opts fs.MkdirOptions) (fs.Directory, error) {
	target := cleanPath(p)
	if target == "/" {
		return l.Root(ctx)
	}
	parentPath, name := splitParent(target)
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		if opts.Parents && errors.Is(err, fs.ErrNotFound) {
			if _, err := l.Mkdir(ctx, parentPath, fs.MkdirOptions{Parents: true, Mode: opts.Mode}); err != nil {
				return nil, err
			}
			parentDir, err = l.resolveDir(ctx, parentPath)
		}
		if err != nil {
			return nil, err
		}
	}
	if _, err := l.child(ctx, parentDir.inode.ID, name); err == nil {
		return nil, fs.ErrAlreadyExist
	}
	node, err := l.newDirectoryInode(ctx, parentDir.inode.ID, name, opts)
	if err != nil {
		return nil, err
	}
	l.cachePut(target, node)
	return &localDirectory{fs: l, inode: node, path: target}, nil
}

func (l *LocalFs) Remove(ctx context.Context, p string, opts fs.RemoveOptions) error {
	target := cleanPath(p)
	if target == "/" {
		return fmt.Errorf("cannot remove root")
	}
	parentPath, name := splitParent(target)
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		return err
	}
	inode, err := l.child(ctx, parentDir.inode.ID, name)
	if err != nil {
		if opts.Force {
			return nil
		}
		return err
	}
	if inode.Type == meta.TypeDirectory {
		children, err := l.store.Children(ctx, inode.ID)
		if err != nil {
			return err
		}
		if len(children) > 0 && !opts.Recursive {
			return fmt.Errorf("directory not empty: %s", target)
		}
		if opts.Recursive {
			for _, child := range children {
				childPath := path.Join(target, child.Name)
				if err := l.Remove(ctx, childPath, opts); err != nil {
					return err
				}
			}
		}
	}
	l.cacheDelete(target)
	return l.unlink(ctx, parentDir.inode.ID, name, inode)
}

func (l *LocalFs) OpenFile(ctx context.Context, p string, flags fs.OpenFlags, perm os.FileMode, user fs.User) (fs.Object, error) {
	target := cleanPath(p)
	create := flags&fs.OpenFlagCreate != 0
	write := flags&(fs.OpenFlagWriteOnly|fs.OpenFlagReadWrite) != 0
	exclusive := flags&fs.OpenFlagExclusive != 0
	truncate := flags&fs.OpenFlagTruncate != 0

	obj, err := l.Stat(ctx, target)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) && create {
			newObj, err := l.Create(ctx, target, fs.CreateOptions{Mode: uint32(perm), Overwrite: !exclusive})
			if err != nil {
				return nil, err
			}
			if truncate && write {
				if err := newObj.Truncate(ctx, 0); err != nil {
					return nil, err
				}
			}
			return newObj, nil
		}
		return nil, err
	}
	if exclusive && create {
		return nil, fs.ErrAlreadyExist
	}
	if truncate && write {
		if err := obj.Truncate(ctx, 0); err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func (l *LocalFs) Rename(ctx context.Context, oldPath, newPath string, opts fs.RenameOptions, user fs.User) error {
	src := cleanPath(oldPath)
	dst := cleanPath(newPath)
	if src == dst {
		return nil
	}
	if src == "/" {
		return fmt.Errorf("cannot rename root")
	}
	srcParentPath, srcName := splitParent(src)
	dstParentPath, dstName := splitParent(dst)
	srcParent, err := l.resolveDir(ctx, srcParentPath)
	if err != nil {
		return err
	}
	dstParent, err := l.resolveDir(ctx, dstParentPath)
	if err != nil {
		return err
	}
	child, err := l.child(ctx, srcParent.inode.ID, srcName)
	if err != nil {
		return err
	}
	existing, err := l.child(ctx, dstParent.inode.ID, dstName)
	if err == nil {
		if opts.NoReplace {
			return fs.ErrAlreadyExist
		}
		if err := l.unlink(ctx, dstParent.inode.ID, dstName, existing); err != nil {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotFound) {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	inode, err := l.store.Get(ctx, child.ID)
	if err != nil {
		return err
	}
	if inode.Parents == nil {
		inode.Parents = make(map[fs.ID]map[string]struct{})
	}
	if set, ok := inode.Parents[srcParent.inode.ID]; ok {
		delete(set, srcName)
		if len(set) == 0 {
			delete(inode.Parents, srcParent.inode.ID)
		}
	}
	set := inode.Parents[dstParent.inode.ID]
	if set == nil {
		set = make(map[string]struct{})
	}
	set[dstName] = struct{}{}
	inode.Parents[dstParent.inode.ID] = set
	inode.Parent = dstParent.inode.ID
	inode.Name = dstName
	if err := l.store.Put(ctx, inode); err != nil {
		return err
	}
	l.cacheDeletePrefix(src)
	l.cachePut(dst, inode)
	return nil
}

func (l *LocalFs) SetAttr(ctx context.Context, p string, changes fs.AttrChanges, user fs.User) error {
	target := cleanPath(p)
	inode, err := l.resolve(ctx, target)
	if err != nil {
		return err
	}
	updated := inode
	if changes.Mode != nil {
		updated.Mode = uint32(*changes.Mode)
	}
	if changes.UID != nil {
		updated.UID = *changes.UID
	}
	if changes.GID != nil {
		updated.GID = *changes.GID
	}
	if changes.MTime != nil {
		updated.MTime = *changes.MTime
	}
	if changes.CTime != nil {
		updated.CTime = *changes.CTime
	}
	if changes.XAttrs != nil {
		if updated.Metadata == nil {
			updated.Metadata = make(map[string]string)
		}
		for k, v := range changes.XAttrs {
			updated.Metadata[k] = string(v)
		}
	}
	if changes.Size != nil && updated.Type == meta.TypeFile {
		obj, err := l.Stat(ctx, target)
		if err != nil {
			return err
		}
		if err := obj.Truncate(ctx, *changes.Size); err != nil {
			return err
		}
		updated.Size = *changes.Size
	}
	if err := l.store.Put(ctx, updated); err != nil {
		return err
	}
	l.cachePut(target, updated)
	return nil
}

func (l *LocalFs) Access(ctx context.Context, p string, mode fs.AccessMode, user fs.User) error {
	if _, err := l.resolve(ctx, p); err != nil {
		return err
	}
	// TODO: enforce POSIX permission checks using user identity.
	return nil
}

func (l *LocalFs) Mknod(ctx context.Context, path string, kind fs.SpecialKind, perm os.FileMode, dev fs.DeviceNumber, user fs.User) error {
	return fs.ErrNotSupported
}

func (l *LocalFs) Mkfifo(ctx context.Context, path string, perm os.FileMode, user fs.User) error {
	target := cleanPath(path)
	parentPath, name := splitParent(target)
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		return err
	}
	if _, err := l.child(ctx, parentDir.inode.ID, name); err == nil {
		return fs.ErrAlreadyExist
	}
	inode := meta.Inode{
		ID:        fs.ID(fmt.Sprintf("fifo-%d", time.Now().UnixNano())),
		Parent:    parentDir.inode.ID,
		Name:      name,
		Type:      meta.TypeFile,
		Mode:      uint32(perm),
		UID:       0,
		GID:       0,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
		Metadata: map[string]string{
			"special": "fifo",
		},
		Parents: parentSet(parentDir.inode.ID, name),
	}
	if err := l.store.Put(ctx, inode); err != nil {
		return err
	}
	l.cachePut(target, inode)
	return nil
}

func (l *LocalFs) Symlink(ctx context.Context, target, link string, user fs.User) error {
	parentPath, name := splitParent(link)
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		return err
	}
	if _, err := l.child(ctx, parentDir.inode.ID, name); err == nil {
		return fs.ErrAlreadyExist
	}
	inode, err := l.newSymlinkInode(ctx, parentDir.inode.ID, name, target, fs.CreateOptions{Mode: 0o777})
	if err != nil {
		return err
	}
	l.cachePut(cleanPath(link), inode)
	return nil
}

func (l *LocalFs) Readlink(ctx context.Context, link string, user fs.User) (string, error) {
	inode, err := l.resolve(ctx, link)
	if err != nil {
		return "", err
	}
	if inode.Type != meta.TypeSymlink {
		return "", fs.ErrNotSupported
	}
	return inode.Target, nil
}

func (l *LocalFs) Lock(ctx context.Context, path string, opts fs.LockOptions, user fs.User) error {
	target := cleanPath(path)
	owner := opts.Owner
	if owner == "" {
		owner = fmt.Sprintf("%d:%d", user.UID, user.GID)
	}
	l.lockMu.Lock()
	defer l.lockMu.Unlock()
	record := l.locks[target]
	if record == nil {
		l.locks[target] = &lockRecord{owner: owner, exclusive: opts.Exclusive, ref: 1}
		return nil
	}
	if record.owner != owner {
		if record.exclusive || opts.Exclusive {
			return fmt.Errorf("lock held by %s", record.owner)
		}
	}
	if opts.Exclusive && !record.exclusive && record.ref > 0 && record.owner == owner {
		if record.ref > 1 {
			return fmt.Errorf("lock upgrade not supported")
		}
		record.exclusive = true
		return nil
	}
	if record.exclusive && record.owner != owner {
		return fmt.Errorf("lock held by %s", record.owner)
	}
	record.ref++
	return nil
}

func (l *LocalFs) Unlock(ctx context.Context, path string, opts fs.LockOptions, user fs.User) error {
	target := cleanPath(path)
	owner := opts.Owner
	if owner == "" {
		owner = fmt.Sprintf("%d:%d", user.UID, user.GID)
	}
	l.lockMu.Lock()
	defer l.lockMu.Unlock()
	record := l.locks[target]
	if record == nil {
		return fmt.Errorf("lock not held")
	}
	if record.owner != owner {
		return fmt.Errorf("lock held by %s", record.owner)
	}
	record.ref--
	if record.ref <= 0 {
		delete(l.locks, target)
	}
	return nil
}

func (l *LocalFs) List(ctx context.Context, p string, opts fs.ListOptions) (<-chan fs.Entry, error) {
	dir, err := l.resolveDir(ctx, p)
	if err != nil {
		return nil, err
	}
	ch := make(chan fs.Entry)
	go func() {
		defer close(ch)
		l.streamEntries(ctx, dir.path, dir.inode, opts, ch)
	}()
	return ch, nil
}

func (l *LocalFs) Link(ctx context.Context, source, target string, kind fs.LinkKind) error {
	srcPath := cleanPath(source)
	dstPath := cleanPath(target)
	srcInode, err := l.resolve(ctx, srcPath)
	if err != nil {
		return err
	}
	parentPath, name := splitParent(dstPath)
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		return err
	}
	if _, err := l.child(ctx, parentDir.inode.ID, name); err == nil {
		return fs.ErrAlreadyExist
	} else if !errors.Is(err, fs.ErrNotFound) {
		return err
	}
	switch kind {
	case fs.LinkHard:
		if srcInode.Type == meta.TypeDirectory {
			return fs.ErrNotSupported
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		inode, err := l.store.Get(ctx, srcInode.ID)
		if err != nil {
			return err
		}
		if inode.Parents == nil {
			inode.Parents = make(map[fs.ID]map[string]struct{})
		}
		set := inode.Parents[parentDir.inode.ID]
		if set == nil {
			set = make(map[string]struct{})
		}
		if _, exists := set[name]; exists {
			return fs.ErrAlreadyExist
		}
		set[name] = struct{}{}
		inode.Parents[parentDir.inode.ID] = set
		inode.LinkCount++
		if err := l.store.Put(ctx, inode); err != nil {
			return err
		}
		l.cachePut(dstPath, inode)
	case fs.LinkSymbolic:
		opts := fs.CreateOptions{Mode: 0o777, Overwrite: false}
		inode, err := l.newSymlinkInode(ctx, parentDir.inode.ID, name, srcPath, opts)
		if err != nil {
			return err
		}
		l.cachePut(dstPath, inode)
	default:
		return fs.ErrNotSupported
	}
	return nil
}

func (l *LocalFs) Copy(ctx context.Context, source, target string, opts fs.CopyOptions) (fs.Object, error) {
	srcPath := cleanPath(source)
	dstPath := cleanPath(target)
	if srcPath == dstPath {
		return nil, fmt.Errorf("copy: source and destination must differ")
	}
	srcInode, err := l.resolve(ctx, srcPath)
	if err != nil {
		return nil, err
	}
	if srcInode.Type != meta.TypeFile {
		return nil, fs.ErrNotSupported
	}
	parentPath, name := splitParent(dstPath)
	if name == "" {
		return nil, fmt.Errorf("copy: destination must include file name")
	}
	parentDir, err := l.resolveDir(ctx, parentPath)
	if err != nil {
		return nil, err
	}
	existing, err := l.child(ctx, parentDir.inode.ID, name)
	if err == nil {
		if !opts.Overwrite {
			return nil, fs.ErrAlreadyExist
		}
		if existing.Type == meta.TypeDirectory {
			return nil, fmt.Errorf("copy: cannot overwrite directory %s", dstPath)
		}
		if err := l.unlink(ctx, parentDir.inode.ID, name, existing); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, fs.ErrNotFound) {
		return nil, err
	}
	id, err := l.store.AllocateID(ctx)
	if err != nil {
		return nil, err
	}
	mode := srcInode.Mode
	if opts.Mode != nil {
		mode = *opts.Mode
	}
	metadata := cloneMetadataMap(srcInode.Metadata)
	if opts.Metadata != nil {
		metadata = cloneMetadataMap(opts.Metadata)
	}
	shards := cloneShardRefs(srcInode.Shards)
	now := time.Now()
	newInode := meta.Inode{
		ID:        id,
		Parent:    parentDir.inode.ID,
		Name:      name,
		Type:      meta.TypeFile,
		Mode:      mode,
		UID:       srcInode.UID,
		GID:       srcInode.GID,
		MTime:     now,
		CTime:     now,
		LinkCount: 1,
		Shards:    shards,
		Size:      srcInode.Size,
		Metadata:  metadata,
		Parents:   parentSet(parentDir.inode.ID, name),
	}
	if err := l.store.Put(ctx, newInode); err != nil {
		return nil, err
	}
	for _, shard := range shards {
		_ = l.tracker.Add(ctx, shard.ShardID, 1)
	}
	l.cachePut(dstPath, newInode)
	return &localObject{fs: l, inode: newInode, path: dstPath}, nil
}

func (l *LocalFs) streamEntries(ctx context.Context, basePath string, dir meta.Inode, opts fs.ListOptions, ch chan<- fs.Entry) {
	var count int
	l.walkEntries(ctx, basePath, dir, opts, ch, &count, true)
}

func (l *LocalFs) walkEntries(ctx context.Context, basePath string, dir meta.Inode, opts fs.ListOptions, ch chan<- fs.Entry, count *int, applyMarker bool) {
	if opts.Limit > 0 && *count >= opts.Limit {
		return
	}
	children, err := l.store.Children(ctx, dir.ID)
	if err != nil {
		return
	}
	sort.Slice(children, func(i, j int) bool {
		return children[i].Name < children[j].Name
	})
	for _, child := range children {
		if opts.Limit > 0 && *count >= opts.Limit {
			return
		}
		if applyMarker && opts.StartAfter != "" && child.Name <= opts.StartAfter {
			continue
		}
		if !opts.IncludeHidden && strings.HasPrefix(child.Name, ".") {
			continue
		}
		childPath := path.Join(basePath, child.Name)
		switch child.Type {
		case meta.TypeDirectory:
			entry := fs.Entry{Dir: &localDirectory{fs: l, inode: child, path: childPath}, Name: child.Name}
			ch <- entry
			*count++
			if opts.Limit > 0 && *count >= opts.Limit {
				return
			}
			if opts.Recursive {
				l.walkEntries(ctx, childPath, child, opts, ch, count, false)
				if opts.Limit > 0 && *count >= opts.Limit {
					return
				}
			}
		case meta.TypeFile:
			entry := fs.Entry{Object: &localObject{fs: l, inode: child, path: childPath}, Name: child.Name}
			ch <- entry
			*count++
		case meta.TypeSymlink:
			entry := fs.Entry{Link: &fs.Link{Kind: fs.LinkSymbolic, Target: child.Target, InodeID: child.ID}, Name: child.Name}
			ch <- entry
			*count++
		}
		if opts.Limit > 0 && *count >= opts.Limit {
			return
		}
	}
}

func (l *LocalFs) resolveDir(ctx context.Context, p string) (*localDirectory, error) {
	inode, err := l.resolve(ctx, p)
	if err != nil {
		return nil, err
	}
	if inode.Type != meta.TypeDirectory {
		return nil, fmt.Errorf("not a directory: %s", p)
	}
	return &localDirectory{fs: l, inode: inode, path: cleanPath(p)}, nil
}

func (l *LocalFs) resolve(ctx context.Context, p string) (meta.Inode, error) {
	cleaned := cleanPath(p)
	if cleaned == "/" {
		return l.store.Root(ctx)
	}
	if l.cacheEnabled {
		if v, ok := l.metaCache.Get(cleaned); ok {
			if inode, ok := v.(meta.Inode); ok {
				return inode, nil
			}
		}
	}
	parts := splitParts(cleaned)
	current, err := l.store.Root(ctx)
	if err != nil {
		return meta.Inode{}, err
	}
	currPath := "/"
	for _, part := range parts {
		current, err = l.child(ctx, current.ID, part)
		if err != nil {
			return meta.Inode{}, err
		}
		currPath = path.Join(currPath, part)
	}
	l.cachePut(cleaned, current)
	return current, nil
}

func (l *LocalFs) child(ctx context.Context, parent fs.ID, name string) (meta.Inode, error) {
	children, err := l.store.Children(ctx, parent)
	if err != nil {
		return meta.Inode{}, err
	}
	for _, inode := range children {
		if inode.Name == name {
			return inode, nil
		}
	}
	return meta.Inode{}, fs.ErrNotFound
}

func (l *LocalFs) newFileInode(ctx context.Context, parent fs.ID, name string, opts fs.CreateOptions) (meta.Inode, error) {
	id, err := l.store.AllocateID(ctx)
	if err != nil {
		return meta.Inode{}, err
	}
	inode := meta.Inode{
		ID:        id,
		Parent:    parent,
		Name:      name,
		Type:      meta.TypeFile,
		Mode:      opts.Mode,
		UID:       0,
		GID:       0,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
		Metadata:  opts.Metadata,
		Parents:   parentSet(parent, name),
	}
	if err := l.store.Put(ctx, inode); err != nil {
		return meta.Inode{}, err
	}
	return inode, nil
}

func (l *LocalFs) newDirectoryInode(ctx context.Context, parent fs.ID, name string, opts fs.MkdirOptions) (meta.Inode, error) {
	id, err := l.store.AllocateID(ctx)
	if err != nil {
		return meta.Inode{}, err
	}
	inode := meta.Inode{
		ID:        id,
		Parent:    parent,
		Name:      name,
		Type:      meta.TypeDirectory,
		Mode:      opts.Mode,
		UID:       0,
		GID:       0,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
		Metadata:  opts.Metadata,
		Parents:   parentSet(parent, name),
	}
	if err := l.store.Put(ctx, inode); err != nil {
		return meta.Inode{}, err
	}
	return inode, nil
}

func (l *LocalFs) newSymlinkInode(ctx context.Context, parent fs.ID, name, target string, opts fs.CreateOptions) (meta.Inode, error) {
	id, err := l.store.AllocateID(ctx)
	if err != nil {
		return meta.Inode{}, err
	}
	inode := meta.Inode{
		ID:        id,
		Parent:    parent,
		Name:      name,
		Type:      meta.TypeSymlink,
		Mode:      opts.Mode,
		UID:       0,
		GID:       0,
		MTime:     time.Now(),
		CTime:     time.Now(),
		LinkCount: 1,
		Metadata:  opts.Metadata,
		Target:    target,
		Parents:   parentSet(parent, name),
	}
	if err := l.store.Put(ctx, inode); err != nil {
		return meta.Inode{}, err
	}
	return inode, nil
}

func (l *LocalFs) unlink(ctx context.Context, parent fs.ID, name string, inode meta.Inode) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	stored, err := l.store.Get(ctx, inode.ID)
	if err != nil {
		return err
	}
	if stored.Parents != nil {
		if set, ok := stored.Parents[parent]; ok {
			delete(set, name)
			if len(set) == 0 {
				delete(stored.Parents, parent)
			} else {
				stored.Parents[parent] = set
			}
		}
	}
	stored.LinkCount--
	if stored.LinkCount <= 0 {
		for _, shard := range stored.Shards {
			_ = l.tracker.Release(ctx, shard.ShardID)
		}
		return l.store.Delete(ctx, stored.ID)
	}
	return l.store.Put(ctx, stored)
}

// RunGC performs a synchronous garbage-collection pass.
func (l *LocalFs) RunGC(ctx context.Context) (int, error) {
	if l.gcSweeper == nil {
		return 0, nil
	}
	return l.gcSweeper.Sweep(ctx)
}

// Close stops background helpers.
func (l *LocalFs) Close() error {
	if l.gcCancel != nil {
		l.gcCancel()
		l.gcCancel = nil
	}
	return nil
}

func (l *LocalFs) cachePut(p string, inode meta.Inode) {
	if !l.cacheEnabled {
		return
	}
	l.metaCache.Set(cleanPath(p), inode)
}

func (l *LocalFs) cacheDelete(p string) {
	if !l.cacheEnabled {
		return
	}
	l.metaCache.Delete(cleanPath(p))
}

func (l *LocalFs) cacheDeletePrefix(prefix string) {
	if !l.cacheEnabled {
		return
	}
	cleaned := cleanPath(prefix)
	if cleaned == "/" {
		l.metaCache.Clear()
		return
	}
	l.metaCache.DeletePrefix(cleaned)
}

func splitParts(p string) []string {
	cleaned := strings.TrimPrefix(p, "/")
	if cleaned == "" {
		return nil
	}
	return strings.Split(cleaned, "/")
}

func splitParent(p string) (string, string) {
	cleaned := cleanPath(p)
	if cleaned == "/" {
		return "/", ""
	}
	dir, base := path.Split(cleaned)
	if dir == "" {
		dir = "/"
	}
	if dir != "/" {
		dir = strings.TrimSuffix(dir, "/")
	}
	return dir, base
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	cleaned := path.Clean(p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

// localObject implements fs.Object backed by LocalFs.
type localObject struct {
	fs    *LocalFs
	inode meta.Inode
	path  string
}

func (o *localObject) ID() fs.ID             { return o.inode.ID }
func (o *localObject) Path() string          { return o.path }
func (o *localObject) Size() int64           { return o.inode.Size }
func (o *localObject) ModTime() time.Time    { return o.inode.MTime }
func (o *localObject) Metadata() fs.Metadata { return toMetadata(o.inode) }

func (o *localObject) Read(ctx context.Context, w io.WriterAt, opts fs.IOOptions) (int64, error) {
	if err := o.refresh(ctx); err != nil {
		return 0, err
	}
	shards := toShards(o.inode.Shards)
	readOpts := sharder.ReaderOptions{Key: o.fs.writerOpts.Key, Concurrency: opts.Concurrency}
	return sharder.Concat(ctx, o.fs.blobs, shards, w, readOpts)
}

func (o *localObject) ReadAt(ctx context.Context, buf []byte, offset int64, opts fs.IOOptions) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if offset < 0 {
		return 0, fmt.Errorf("negative offset %d", offset)
	}
	if err := o.refresh(ctx); err != nil {
		return 0, err
	}
	if offset >= o.inode.Size {
		return 0, io.EOF
	}
	requestLen := len(buf)
	remaining := o.inode.Size - offset
	limit := int64(requestLen)
	if limit > remaining {
		limit = remaining
	}
	if limit <= 0 {
		return 0, io.EOF
	}
	target := buf[:limit]
	writer := newRangeWriterAt(target, offset)
	startIdx, baseOffset := shardIndexForOffset(o.inode.Shards, offset)
	if startIdx >= len(o.inode.Shards) {
		return 0, io.EOF
	}
	readerOpts := sharder.ReaderOptions{
		Key:         o.fs.writerOpts.Key,
		Concurrency: opts.Concurrency,
	}
	_, err := sharder.Concat(ctx, o.fs.blobs, toShards(o.inode.Shards[startIdx:]), &offsetWriterAt{
		base: baseOffset,
		dst:  writer,
	}, readerOpts)
	switch {
	case errors.Is(err, errRangeSatisfied):
		err = nil
	case err != nil:
		return writer.Written(), err
	}
	n := writer.Written()
	if int64(requestLen) > limit {
		return n, io.EOF
	}
	return n, nil
}

func (o *localObject) Write(ctx context.Context, r io.Reader, opts fs.IOOptions) (int64, error) {
	if err := o.refresh(ctx); err != nil {
		return 0, err
	}
	total, err := o.fs.writeShards(ctx, &o.inode, r, opts)
	if err != nil {
		return 0, err
	}
	o.fs.cachePut(o.path, o.inode)
	return total, nil
}

func (o *localObject) WriteAt(ctx context.Context, r io.Reader, offset int64, opts fs.IOOptions) (int64, error) {
	if err := o.refresh(ctx); err != nil {
		return 0, err
	}
	existing := &bufferWriterAt{}
	if len(o.inode.Shards) > 0 {
		if _, err := sharder.Concat(ctx, o.fs.blobs, toShards(o.inode.Shards), existing, sharder.ReaderOptions{
			Key:         o.fs.writerOpts.Key,
			Concurrency: opts.Concurrency,
		}); err != nil {
			return 0, err
		}
	}
	payload, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	data := existing.Bytes()
	required := offset + int64(len(payload))
	if int64(len(data)) < required {
		expanded := make([]byte, required)
		copy(expanded, data)
		data = expanded
	} else {
		data = append([]byte(nil), data...)
	}
	copy(data[offset:], payload)
	return o.Write(ctx, bytes.NewReader(data), opts)
}

func (o *localObject) Truncate(ctx context.Context, size int64) error {
	if err := o.refresh(ctx); err != nil {
		return err
	}
	buf := &bufferWriterAt{}
	if _, err := sharder.Concat(ctx, o.fs.blobs, toShards(o.inode.Shards), buf, sharder.ReaderOptions{
		Key:         o.fs.writerOpts.Key,
		Concurrency: 1,
	}); err != nil {
		return err
	}
	data := buf.Bytes()
	if int64(len(data)) > size {
		data = data[:size]
	} else if int64(len(data)) < size {
		pad := make([]byte, size-int64(len(data)))
		data = append(data, pad...)
	}
	_, err := o.Write(ctx, bytes.NewReader(data), fs.IOOptions{})
	return err
}

func (o *localObject) Flush(ctx context.Context) error { return nil }

func (o *localObject) refresh(ctx context.Context) error {
	inode, err := o.fs.store.Get(ctx, o.inode.ID)
	if err != nil {
		return err
	}
	o.inode = inode
	return nil
}

// localDirectory implements fs.Directory.
type localDirectory struct {
	fs    *LocalFs
	inode meta.Inode
	path  string
}

func (d *localDirectory) ID() fs.ID             { return d.inode.ID }
func (d *localDirectory) Path() string          { return d.path }
func (d *localDirectory) Metadata() fs.Metadata { return toMetadata(d.inode) }

func (d *localDirectory) Entries(ctx context.Context, opts fs.ListOptions) (<-chan fs.Entry, error) {
	return d.fs.List(ctx, d.path, opts)
}

func (d *localDirectory) CreateFile(ctx context.Context, name string, opts fs.CreateOptions) (fs.Object, error) {
	return d.fs.Create(ctx, path.Join(d.path, name), opts)
}

func (d *localDirectory) Mkdir(ctx context.Context, name string, opts fs.MkdirOptions) (fs.Directory, error) {
	return d.fs.Mkdir(ctx, path.Join(d.path, name), opts)
}

func (d *localDirectory) Remove(ctx context.Context, name string, opts fs.RemoveOptions) error {
	return d.fs.Remove(ctx, path.Join(d.path, name), opts)
}

// Helpers

func toMetadata(inode meta.Inode) fs.Metadata {
	return fs.Metadata{
		Mode:   inode.Mode,
		UID:    inode.UID,
		GID:    inode.GID,
		MTime:  inode.MTime,
		CTime:  inode.CTime,
		Extras: inode.Metadata,
	}
}

func toShards(refs []meta.ShardRef) []sharder.Shard {
	out := make([]sharder.Shard, 0, len(refs))
	for _, ref := range refs {
		out = append(out, sharder.Shard{ID: blob.ID(ref.ShardID), Size: ref.Size, Checksum: ref.Checksum, Encrypted: ref.Encrypted})
	}
	return out
}

func shardIndexForOffset(refs []meta.ShardRef, offset int64) (int, int64) {
	var consumed int64
	for i, ref := range refs {
		next := consumed + ref.Size
		if next > offset {
			return i, consumed
		}
		consumed = next
	}
	return len(refs), consumed
}

type bufferWriterAt struct {
	buf []byte
	mu  sync.Mutex
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	end := int(off) + len(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if end > len(b.buf) {
		expanded := make([]byte, end)
		copy(expanded, b.buf)
		b.buf = expanded
	}
	copy(b.buf[int(off):end], p)
	return len(p), nil
}

func (b *bufferWriterAt) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf...)
}

var errRangeSatisfied = errors.New("range satisfied")

type rangeWriterAt struct {
	buf     []byte
	start   int64
	end     int64
	written int
}

func newRangeWriterAt(buf []byte, start int64) *rangeWriterAt {
	return &rangeWriterAt{
		buf:   buf,
		start: start,
		end:   start + int64(len(buf)),
	}
}

func (r *rangeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if len(r.buf) == 0 {
		return len(p), errRangeSatisfied
	}
	chunkStart := off
	chunkEnd := off + int64(len(p))
	if chunkEnd <= r.start || chunkStart >= r.end {
		return len(p), nil
	}
	copyStart := max64(chunkStart, r.start)
	copyEnd := min64(chunkEnd, r.end)
	if copyEnd <= copyStart {
		return len(p), nil
	}
	srcOffset := int(copyStart - chunkStart)
	dstOffset := int(copyStart - r.start)
	length := int(copyEnd - copyStart)
	copy(r.buf[dstOffset:dstOffset+length], p[srcOffset:srcOffset+length])
	r.written += length
	if r.written >= len(r.buf) {
		return len(p), errRangeSatisfied
	}
	return len(p), nil
}

func (r *rangeWriterAt) Written() int {
	return r.written
}

type offsetWriterAt struct {
	base int64
	dst  io.WriterAt
}

func (o *offsetWriterAt) WriteAt(p []byte, off int64) (int, error) {
	return o.dst.WriteAt(p, off+o.base)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func cloneShardRefs(refs []meta.ShardRef) []meta.ShardRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]meta.ShardRef, len(refs))
	copy(out, refs)
	return out
}

func cloneMetadataMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (l *LocalFs) writeShards(ctx context.Context, inode *meta.Inode, r io.Reader, opts fs.IOOptions) (int64, error) {
	writerOpts := l.writerOpts
	if opts.ShardSize > 0 {
		writerOpts.ChunkSize = opts.ShardSize
	}
	if opts.Concurrency > 0 {
		writerOpts.Concurrency = opts.Concurrency
	} else {
		writerOpts.Concurrency = 0
	}
	shards, err := sharder.ChunkAndStore(ctx, l.blobs, r, writerOpts)
	if err != nil {
		return 0, err
	}
	var total int64
	var refs []meta.ShardRef
	for idx, shard := range shards {
		total += shard.Size
		refs = append(refs, meta.ShardRef{ShardID: string(shard.ID), Size: shard.Size, Offset: int64(idx), Version: 1, Checksum: shard.Checksum, Encrypted: shard.Encrypted})
		_ = l.tracker.Add(ctx, string(shard.ID), 1)
	}
	var old []string
	for _, ref := range inode.Shards {
		old = append(old, ref.ShardID)
	}
	inode.Shards = refs
	inode.Size = total
	inode.MTime = time.Now()
	if err := l.store.Put(ctx, *inode); err != nil {
		return 0, err
	}
	for _, shardID := range old {
		_ = l.tracker.Release(ctx, shardID)
	}
	return total, nil
}

func parentSet(parent fs.ID, name string) map[fs.ID]map[string]struct{} {
	m := make(map[fs.ID]map[string]struct{})
	names := make(map[string]struct{})
	if name != "" {
		names[name] = struct{}{}
	}
	m[parent] = names
	return m
}

func resolveMetaStore(cfg Config) (meta.Store, error) {
	if cfg.MetaStore != nil {
		return cfg.MetaStore, nil
	}
	if cfg.BoltOptions != nil || cfg.BoltMetadataPath != "" {
		var boltCfg meta.BoltConfig
		if cfg.BoltOptions != nil {
			boltCfg = *cfg.BoltOptions
		}
		if boltCfg.Path == "" {
			boltCfg.Path = cfg.BoltMetadataPath
		}
		if boltCfg.Path == "" {
			return nil, fmt.Errorf("localfs: BoltMetadataPath is required when using Bolt store")
		}
		return meta.NewBoltStore(boltCfg)
	}
	if cfg.MetadataPath != "" {
		return meta.NewFileStore(cfg.MetadataPath)
	}
	return meta.NewMemoryStore(), nil
}

func resolveBlobStore(cfg Config) (blob.Store, error) {
	var primary blob.Store
	var err error
	if cfg.BlobStore != nil {
		primary = cfg.BlobStore
	} else {
		primary, err = blob.NewPathStore(cfg.BlobRoot)
		if err != nil {
			return nil, err
		}
	}
	if cfg.SecondaryStore != nil {
		var opts blob.HybridOptions
		if cfg.HybridOptions != nil {
			opts = *cfg.HybridOptions
		} else {
			opts = blob.HybridOptions{MirrorSecondary: true, CacheOnRead: true}
		}
		return blob.NewHybridStore(primary, cfg.SecondaryStore, opts)
	}
	return primary, nil
}

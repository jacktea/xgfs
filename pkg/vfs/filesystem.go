package vfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jacktea/xgfs/pkg/cache"
	"github.com/jacktea/xgfs/pkg/fs"
)

// Options configures the virtual filesystem wrapper. Fields are reserved for
// future caching/Policy tuning so Phase 2 can focus on wiring.
type Options struct {
	// PassthroughOnly forces the wrapper to return fs.ErrNotSupported for POSIX
	// operations when the backend lacks native support. In later phases this will
	// toggle shims/emulation; for now it simply documents the intent.
	PassthroughOnly bool
	// MetadataCacheSize caps the number of path entries cached in memory (0 disables caching).
	MetadataCacheSize int
	// MetadataCacheTTL defines how long metadata entries stay valid.
	MetadataCacheTTL time.Duration
}

// FS layers additional semantics (caching, POSIX shims, identity propagation)
// on top of any backend fs.Fs. Phase 2 keeps behaviour as a passthrough so
// callers can switch dependencies without breaking compatibility.
type FS struct {
	backend fs.Fs
	posix   PosixFs
	opts    Options

	cacheMu   sync.RWMutex
	metaCache *cache.Cache
}

var _ PosixFs = (*FS)(nil)

// New constructs a virtual filesystem on top of backend. The returned value
// always satisfies fs.Fs and PosixFs (returning fs.ErrNotSupported if the
// backend lacks those capabilities).
func New(backend fs.Fs, opts Options) *FS {
	if backend == nil {
		panic("vfs: backend must not be nil")
	}
	var posix PosixFs
	switch b := backend.(type) {
	case PosixProvider:
		posix = b.PosixAdapter()
	case PosixFs:
		posix = b
	}
	fs := &FS{
		backend: backend,
		posix:   posix,
		opts:    opts,
	}
	if opts.MetadataCacheSize > 0 {
		ttl := opts.MetadataCacheTTL
		if ttl <= 0 {
			ttl = 5 * time.Second
		}
		fs.metaCache = cache.New(opts.MetadataCacheSize, ttl)
	}
	return fs
}

// Backend exposes the wrapped fs.Fs for callers that still need direct access.
func (f *FS) Backend() fs.Fs { return f.backend }

func (f *FS) Name() string          { return f.backend.Name() }
func (f *FS) Features() fs.Features { return f.backend.Features() }
func (f *FS) Root(ctx context.Context) (fs.Directory, error) {
	return f.backend.Root(ctx)
}
func (f *FS) Stat(ctx context.Context, path string) (fs.Object, error) {
	obj, err := f.backend.Stat(ctx, path)
	if err == nil {
		f.cachePut(cleanPath(path), obj.Metadata(), false)
	}
	return obj, err
}
func (f *FS) Create(ctx context.Context, path string, opts fs.CreateOptions) (fs.Object, error) {
	user := UserFromContext(ctx)
	if err := f.ensureDirPerm(ctx, parentPath(path), AccessWrite|AccessExec, user); err != nil {
		return nil, err
	}
	obj, err := f.backend.Create(ctx, path, opts)
	if err == nil {
		f.cacheDelete(path)
		f.cacheDelete(parentPath(path))
	}
	return obj, err
}
func (f *FS) Mkdir(ctx context.Context, path string, opts fs.MkdirOptions) (fs.Directory, error) {
	user := UserFromContext(ctx)
	if err := f.ensureDirPerm(ctx, parentPath(path), AccessWrite|AccessExec, user); err != nil {
		return nil, err
	}
	dir, err := f.backend.Mkdir(ctx, path, opts)
	if err == nil {
		f.cacheDelete(path)
		f.cacheDelete(parentPath(path))
	}
	return dir, err
}
func (f *FS) Remove(ctx context.Context, path string, opts fs.RemoveOptions) error {
	user := UserFromContext(ctx)
	parent := parentPath(path)
	if err := f.ensureDirPerm(ctx, parent, AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := f.enforceSticky(ctx, parent, path, user); err != nil {
		return err
	}
	err := f.backend.Remove(ctx, path, opts)
	if err == nil {
		f.cacheDelete(path)
		f.cacheDelete(parent)
	}
	return err
}
func (f *FS) List(ctx context.Context, path string, opts fs.ListOptions) (<-chan fs.Entry, error) {
	return f.backend.List(ctx, path, opts)
}
func (f *FS) Link(ctx context.Context, source, target string, kind fs.LinkKind) error {
	user := UserFromContext(ctx)
	if err := f.ensureDirPerm(ctx, parentPath(target), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if kind == fs.LinkHard {
		if err := f.Access(ctx, source, AccessRead, user); err != nil {
			return err
		}
	}
	err := f.backend.Link(ctx, source, target, kind)
	if err == nil {
		f.cacheDelete(target)
		f.cacheDelete(parentPath(target))
	}
	return err
}
func (f *FS) Copy(ctx context.Context, source, target string, opts fs.CopyOptions) (fs.Object, error) {
	return f.backend.Copy(ctx, source, target, opts)
}

func (f *FS) OpenFile(ctx context.Context, path string, flags OpenFlags, perm os.FileMode, user User) (fs.Object, error) {
	posix, err := f.posixBackend()
	if err != nil {
		return nil, err
	}
	return posix.OpenFile(ctx, path, flags, perm, user)
}

func (f *FS) Rename(ctx context.Context, oldPath, newPath string, opts RenameOptions, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	if err := f.ensureDirPerm(ctx, parentPath(oldPath), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := f.ensureDirPerm(ctx, parentPath(newPath), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := posix.Rename(ctx, oldPath, newPath, opts, user); err != nil {
		return err
	}
	f.cacheDelete(oldPath)
	f.cacheDelete(newPath)
	f.cacheDelete(parentPath(oldPath))
	f.cacheDelete(parentPath(newPath))
	return nil
}

func (f *FS) SetAttr(ctx context.Context, path string, changes AttrChanges, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	if err := f.requireOwnership(ctx, path, user); err != nil {
		return err
	}
	if err := posix.SetAttr(ctx, path, changes, user); err != nil {
		return err
	}
	f.cacheDelete(path)
	return nil
}

func (f *FS) Access(ctx context.Context, path string, mode AccessMode, user User) error {
	if mode == 0 {
		return nil
	}
	if mode&AccessExists != 0 {
		mode &^= AccessExists
		if mode == 0 {
			_, _, err := f.metadataForPath(ctx, path)
			return err
		}
	}
	meta, isDir, err := f.metadataForPath(ctx, path)
	if err != nil {
		// Fall back to backend-specific Access implementation if available.
		if f.posix != nil && errors.Is(err, fs.ErrNotSupported) {
			return f.posix.Access(ctx, path, mode, user)
		}
		return err
	}
	if hasPermissions(meta, user, mode, isDir) {
		return nil
	}
	return os.ErrPermission
}

func (f *FS) Mknod(ctx context.Context, path string, kind SpecialKind, perm os.FileMode, dev DeviceNumber, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	if err := f.ensureDirPerm(ctx, parentPath(path), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := posix.Mknod(ctx, path, kind, perm, dev, user); err != nil {
		return err
	}
	f.cacheDelete(path)
	f.cacheDelete(parentPath(path))
	return nil
}

func (f *FS) Mkfifo(ctx context.Context, path string, perm os.FileMode, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	if err := f.ensureDirPerm(ctx, parentPath(path), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := posix.Mkfifo(ctx, path, perm, user); err != nil {
		return err
	}
	f.cacheDelete(path)
	f.cacheDelete(parentPath(path))
	return nil
}

func (f *FS) Symlink(ctx context.Context, target, link string, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	if err := f.ensureDirPerm(ctx, parentPath(link), AccessWrite|AccessExec, user); err != nil {
		return err
	}
	if err := posix.Symlink(ctx, target, link, user); err != nil {
		return err
	}
	f.cacheDelete(link)
	f.cacheDelete(parentPath(link))
	return nil
}

func (f *FS) Readlink(ctx context.Context, link string, user User) (string, error) {
	posix, err := f.posixBackend()
	if err != nil {
		return "", err
	}
	return posix.Readlink(ctx, link, user)
}

func (f *FS) Lock(ctx context.Context, path string, opts LockOptions, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	return posix.Lock(ctx, path, opts, user)
}

func (f *FS) Unlock(ctx context.Context, path string, opts LockOptions, user User) error {
	posix, err := f.posixBackend()
	if err != nil {
		return err
	}
	return posix.Unlock(ctx, path, opts, user)
}

func (f *FS) posixBackend() (PosixFs, error) {
	if f.posix == nil {
		return nil, fmt.Errorf("posix op not supported: %w", fs.ErrNotSupported)
	}
	if f.opts.PassthroughOnly {
		return f.posix, nil
	}
	return f.posix, nil
}

func (f *FS) metadataForPath(ctx context.Context, p string) (fs.Metadata, bool, error) {
	clean := cleanPath(p)
	if meta, isDir, ok := f.cacheGet(clean); ok {
		return meta, isDir, nil
	}
	obj, err := f.backend.Stat(ctx, clean)
	if err == nil {
		meta := obj.Metadata()
		f.cachePut(clean, meta, false)
		return meta, false, nil
	}
	if err != nil && !errors.Is(err, fs.ErrNotFound) && !errors.Is(err, fs.ErrNotSupported) {
		return fs.Metadata{}, false, err
	}
	dir, derr := f.resolveDirectory(ctx, clean)
	if derr != nil {
		return fs.Metadata{}, false, derr
	}
	meta := dir.Metadata()
	f.cachePut(clean, meta, true)
	return meta, true, nil
}

func (f *FS) ensureDirPerm(ctx context.Context, dirPath string, mode AccessMode, user User) error {
	if user.UID == 0 {
		return nil
	}
	target := cleanPath(dirPath)
	for {
		meta, _, err := f.metadataForPath(ctx, target)
		if err == nil {
			if hasPermissions(meta, user, mode, true) {
				return nil
			}
			return os.ErrPermission
		}
		if errors.Is(err, fs.ErrNotFound) && target != "/" {
			target = parentPath(target)
			continue
		}
		return err
	}
}

func (f *FS) requireOwnership(ctx context.Context, path string, user User) error {
	if user.UID == 0 {
		return nil
	}
	meta, _, err := f.metadataForPath(ctx, path)
	if err != nil {
		return err
	}
	if meta.UID != user.UID {
		return os.ErrPermission
	}
	return nil
}

func (f *FS) enforceSticky(ctx context.Context, parentPath, childPath string, user User) error {
	if user.UID == 0 {
		return nil
	}
	parentMeta, _, err := f.metadataForPath(ctx, parentPath)
	if err != nil {
		return err
	}
	if os.FileMode(parentMeta.Mode)&os.ModeSticky == 0 {
		return nil
	}
	childMeta, _, err := f.metadataForPath(ctx, childPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return nil
		}
		return err
	}
	if parentMeta.UID == user.UID || childMeta.UID == user.UID {
		return nil
	}
	return os.ErrPermission
}

func (f *FS) resolveDirectory(ctx context.Context, p string) (fs.Directory, error) {
	dir, err := f.backend.Root(ctx)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return dir, nil
	}
	segments := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		entries, err := dir.Entries(ctx, fs.ListOptions{})
		if err != nil {
			return nil, err
		}
		var next fs.Directory
		for entry := range entries {
			if next == nil && entry.Name == seg && entry.Dir != nil {
				next = entry.Dir
			}
		}
		if next == nil {
			return nil, fs.ErrNotFound
		}
		dir = next
	}
	return dir, nil
}

func hasPermissions(meta fs.Metadata, user User, mode AccessMode, isDir bool) bool {
	if mode == 0 {
		return true
	}
	if user.UID == 0 {
		return true
	}
	role := permOther
	if user.UID == meta.UID {
		role = permOwner
	} else if userInGroup(user, meta.GID) {
		role = permGroup
	}
	required := []AccessMode{AccessRead, AccessWrite, AccessExec}
	perms := os.FileMode(meta.Mode)
	for _, req := range required {
		if mode&req == 0 {
			continue
		}
		mask := permissionMask(role, req)
		if perms&mask == 0 {
			return false
		}
	}
	return true
}

type permissionRole int

const (
	permOwner permissionRole = iota
	permGroup
	permOther
)

func permissionMask(role permissionRole, mode AccessMode) os.FileMode {
	switch role {
	case permOwner:
		switch mode {
		case AccessRead:
			return 0o400
		case AccessWrite:
			return 0o200
		case AccessExec:
			return 0o100
		}
	case permGroup:
		switch mode {
		case AccessRead:
			return 0o040
		case AccessWrite:
			return 0o020
		case AccessExec:
			return 0o010
		}
	default:
		switch mode {
		case AccessRead:
			return 0o004
		case AccessWrite:
			return 0o002
		case AccessExec:
			return 0o001
		}
	}
	return 0
}

func userInGroup(user User, gid uint32) bool {
	if user.GID == gid {
		return true
	}
	for _, g := range user.Supplementary {
		if g == gid {
			return true
		}
	}
	return false
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	clean := path.Clean(p)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	if clean == "" {
		return "/"
	}
	return clean
}

func parentPath(p string) string {
	clean := cleanPath(p)
	if clean == "/" {
		return "/"
	}
	return path.Dir(clean)
}

func (f *FS) cacheGet(path string) (fs.Metadata, bool, bool) {
	if f.metaCache == nil {
		return fs.Metadata{}, false, false
	}
	f.cacheMu.RLock()
	value, ok := f.metaCache.Get(path)
	f.cacheMu.RUnlock()
	if !ok {
		return fs.Metadata{}, false, false
	}
	if entry, ok := value.(metaEntry); ok {
		return entry.meta, entry.isDir, true
	}
	return fs.Metadata{}, false, false
}

func (f *FS) cachePut(path string, meta fs.Metadata, isDir bool) {
	if f.metaCache == nil {
		return
	}
	f.cacheMu.Lock()
	f.metaCache.Set(path, metaEntry{meta: meta, isDir: isDir})
	f.cacheMu.Unlock()
}

func (f *FS) cacheDelete(path string) {
	if f.metaCache == nil {
		return
	}
	f.cacheMu.Lock()
	f.metaCache.Delete(path)
	f.cacheMu.Unlock()
}

type metaEntry struct {
	meta  fs.Metadata
	isDir bool
}

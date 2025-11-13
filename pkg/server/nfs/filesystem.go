package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	billy "github.com/go-git/go-billy/v5"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/vfs"
)

type filesystem struct {
	ctx  context.Context
	back posixFilesystem
	root string
}

type posixFilesystem interface {
	fs.Fs
	vfs.PosixFs
}

func newFilesystem(ctx context.Context, backend posixFilesystem, export string) (billy.Filesystem, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if export == "" {
		export = "/"
	}
	export = cleanPath(export)
	fsys := &filesystem{
		ctx:  ctx,
		back: backend,
		root: export,
	}
	if _, err := fsys.ensureDir(export); err != nil {
		return nil, err
	}
	return fsys, nil
}

func (f *filesystem) Create(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
}

func (f *filesystem) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

func (f *filesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	full, err := f.resolve(filename)
	if err != nil {
		return nil, err
	}
	obj, err := f.openObject(full, flag, perm)
	if err != nil {
		return nil, err
	}
	if flag&(os.O_RDWR|os.O_WRONLY) != 0 && flag&os.O_TRUNC != 0 {
		if err := obj.Truncate(f.ctx, 0); err != nil {
			return nil, translateErr(err)
		}
	}
	offset := int64(0)
	if flag&os.O_APPEND != 0 {
		offset = obj.Size()
	}
	return newFile(f.ctx, obj, full, flag, offset), nil
}

func (f *filesystem) Stat(filename string) (os.FileInfo, error) {
	full, err := f.resolve(filename)
	if err != nil {
		return nil, err
	}
	entry, err := f.entryFor(full)
	if err != nil {
		return nil, err
	}
	return entryToInfo(entry), nil
}

func (f *filesystem) Lstat(filename string) (os.FileInfo, error) {
	return f.Stat(filename)
}

func (f *filesystem) Rename(oldpath, newpath string) error {
	oldFull, err := f.resolve(oldpath)
	if err != nil {
		return err
	}
	newFull, err := f.resolve(newpath)
	if err != nil {
		return err
	}
	return translateErr(f.back.Rename(f.ctx, oldFull, newFull, vfs.RenameOptions{}, vfs.User{}))
}

func (f *filesystem) Remove(filename string) error {
	full, err := f.resolve(filename)
	if err != nil {
		return err
	}
	info, err := f.Stat(filename)
	if err != nil {
		return err
	}
	opts := fs.RemoveOptions{}
	if info.IsDir() {
		opts.Recursive = true
	}
	return translateErr(f.back.Remove(f.ctx, full, opts))
}

func (f *filesystem) ReadDir(p string) ([]os.FileInfo, error) {
	full, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	entries, err := f.list(full)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	out := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entryToInfo(&entry))
	}
	return out, nil
}

func (f *filesystem) MkdirAll(filename string, perm os.FileMode) error {
	full, err := f.resolve(filename)
	if err != nil {
		return err
	}
	_, err = f.back.Mkdir(f.ctx, full, fs.MkdirOptions{Parents: true, Mode: uint32(perm)})
	return translateErr(err)
}

func (f *filesystem) Symlink(target, link string) error {
	linkFull, err := f.resolve(link)
	if err != nil {
		return err
	}
	targetFull, err := f.resolve(target)
	if err != nil {
		return err
	}
	return translateErr(f.back.Link(f.ctx, targetFull, linkFull, fs.LinkSymbolic))
}

func (f *filesystem) Readlink(link string) (string, error) {
	full, err := f.resolve(link)
	if err != nil {
		return "", err
	}
	entry, err := f.entryFor(full)
	if err != nil {
		return "", err
	}
	if entry.Link == nil {
		return "", os.ErrInvalid
	}
	return entry.Link.Target, nil
}

func (f *filesystem) TempFile(dir, prefix string) (billy.File, error) {
	if dir == "" {
		dir = "/"
	}
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("%s%d", prefix, rand.Int())
		fullPath := f.Join(dir, name)
		file, err := f.OpenFile(fullPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return file, err
	}
	return nil, fmt.Errorf("tempfile: unable to allocate")
}

func (f *filesystem) Chroot(p string) (billy.Filesystem, error) {
	full, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	return newFilesystem(f.ctx, f.back, full)
}

func (f *filesystem) Root() string {
	return f.root
}

func (f *filesystem) Join(elem ...string) string {
	res := path.Join(elem...)
	if res == "" {
		return "/"
	}
	return res
}

func (f *filesystem) Chmod(string, os.FileMode) error {
	return os.ErrPermission
}

func (f *filesystem) Lchown(string, int, int) error {
	return os.ErrPermission
}

func (f *filesystem) Chown(string, int, int) error {
	return os.ErrPermission
}

func (f *filesystem) Chtimes(string, time.Time, time.Time) error {
	return os.ErrPermission
}

func (f *filesystem) ensureDir(p string) (fs.Directory, error) {
	if p == "/" {
		return f.back.Root(f.ctx)
	}
	parent, name := splitParent(p)
	ch, err := f.back.List(f.ctx, parent, fs.ListOptions{})
	if err != nil {
		return nil, translateErr(err)
	}
	for entry := range ch {
		if entry.Name == name && entry.Dir != nil {
			return entry.Dir, nil
		}
	}
	return nil, os.ErrNotExist
}

func (f *filesystem) openObject(full string, flag int, perm os.FileMode) (fs.Object, error) {
	obj, err := f.back.Stat(f.ctx, full)
	if err == nil {
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
		return obj, nil
	}
	if errors.Is(err, fs.ErrNotFound) {
		if flag&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		return f.back.Create(f.ctx, full, fs.CreateOptions{Mode: uint32(perm), Overwrite: false})
	}
	return nil, translateErr(err)
}

func (f *filesystem) resolve(p string) (string, error) {
	if p == "" {
		p = "."
	}
	clean := cleanPath(p)
	var combined string
	if f.root == "/" {
		combined = clean
	} else {
		combined = path.Join(f.root, strings.TrimPrefix(clean, "/"))
		if !strings.HasPrefix(combined, f.root) {
			return "", os.ErrPermission
		}
	}
	if combined == "" {
		combined = "/"
	}
	return combined, nil
}

func (f *filesystem) entryFor(full string) (*fs.Entry, error) {
	if full == "/" {
		dir, err := f.back.Root(f.ctx)
		if err != nil {
			return nil, translateErr(err)
		}
		return &fs.Entry{Name: "/", Dir: dir}, nil
	}
	parent, name := splitParent(full)
	ch, err := f.back.List(f.ctx, parent, fs.ListOptions{})
	if err != nil {
		return nil, translateErr(err)
	}
	for entry := range ch {
		if entry.Name == name {
			e := entry
			return &e, nil
		}
	}
	return nil, os.ErrNotExist
}

func (f *filesystem) list(full string) ([]fs.Entry, error) {
	ch, err := f.back.List(f.ctx, full, fs.ListOptions{})
	if err != nil {
		return nil, translateErr(err)
	}
	var entries []fs.Entry
	for entry := range ch {
		entries = append(entries, entry)
	}
	return entries, nil
}

func splitParent(p string) (string, string) {
	if p == "/" {
		return "/", "/"
	}
	dir, name := path.Split(p)
	if dir == "" {
		dir = "/"
	}
	dir = cleanPath(dir)
	return dir, strings.TrimSuffix(name, "/")
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	res := path.Clean("/" + strings.TrimSpace(p))
	if res == "" {
		return "/"
	}
	return res
}

func translateErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, fs.ErrNotFound):
		return os.ErrNotExist
	case errors.Is(err, fs.ErrAlreadyExist):
		return os.ErrExist
	default:
		return err
	}
}

type entryInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (e entryInfo) Name() string       { return e.name }
func (e entryInfo) Size() int64        { return e.size }
func (e entryInfo) Mode() os.FileMode  { return e.mode }
func (e entryInfo) ModTime() time.Time { return e.modTime }
func (e entryInfo) IsDir() bool        { return e.isDir }
func (e entryInfo) Sys() interface{}   { return e.sys }

func entryToInfo(entry *fs.Entry) os.FileInfo {
	switch {
	case entry.Dir != nil:
		meta := entry.Dir.Metadata()
		mode := os.FileMode(meta.Mode)
		if mode == 0 {
			mode = os.ModeDir | 0o755
		}
		mode |= os.ModeDir
		return entryInfo{
			name:    entry.Name,
			mode:    mode,
			isDir:   true,
			modTime: meta.MTime,
			sys:     meta,
		}
	case entry.Object != nil:
		meta := entry.Object.Metadata()
		mode := os.FileMode(meta.Mode)
		if mode == 0 {
			mode = 0o644
		}
		return entryInfo{
			name:    entry.Name,
			size:    entry.Object.Size(),
			mode:    mode,
			modTime: entry.Object.ModTime(),
			sys:     meta,
		}
	case entry.Link != nil:
		return entryInfo{
			name:  entry.Name,
			mode:  os.ModeSymlink | 0o777,
			sys:   entry.Link,
			isDir: false,
		}
	default:
		return entryInfo{name: entry.Name}
	}
}

type file struct {
	mu     sync.Mutex
	ctx    context.Context
	obj    fs.Object
	path   string
	flag   int
	offset int64
	closed bool
}

func newFile(ctx context.Context, obj fs.Object, path string, flag int, offset int64) *file {
	if ctx == nil {
		ctx = context.Background()
	}
	return &file{
		ctx:    ctx,
		obj:    obj,
		path:   path,
		flag:   flag,
		offset: offset,
	}
}

func (f *file) Name() string { return f.path }

func (f *file) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	n, err := f.obj.ReadAt(f.ctx, p, f.offset, fs.IOOptions{})
	f.offset += int64(n)
	if err == nil || errors.Is(err, io.EOF) {
		return n, err
	}
	return n, err
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.obj.ReadAt(f.ctx, p, off, fs.IOOptions{})
}

func (f *file) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.flag&os.O_WRONLY == 0 && f.flag&os.O_RDWR == 0 {
		return 0, os.ErrPermission
	}
	n, err := f.obj.WriteAt(f.ctx, bytes.NewReader(p), f.offset, fs.IOOptions{})
	f.offset += n
	return int(n), err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = f.obj.Size() + offset
	default:
		return 0, os.ErrInvalid
	}
	if newOffset < 0 {
		return f.offset, os.ErrInvalid
	}
	f.offset = newOffset
	return f.offset, nil
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return f.obj.Flush(f.ctx)
}

func (f *file) Lock() error   { return nil }
func (f *file) Unlock() error { return nil }

func (f *file) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	return f.obj.Truncate(f.ctx, size)
}

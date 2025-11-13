//go:build linux

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	stdpath "path"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	xfs "github.com/jacktea/xgfs/pkg/fs"
)

const (
	attrTimeout    = 2 * time.Second
	entryTimeout   = 2 * time.Second
	defaultBlkSz   = 4096
	defaultDirMod  = 0o755
	defaultFileMod = 0o644
	defaultLinkMod = 0o777
)

// Mount wires the repository fs.Fs into a FUSE mountpoint.
func Mount(ctx context.Context, filesystem xfs.Fs, mountpoint string) error {
	if filesystem == nil {
		return fmt.Errorf("fuse: nil filesystem")
	}
	root := newDirNode(filesystem, "/")
	server, err := gofuse.Mount(mountpoint, root, &gofuse.Options{
		MountOptions: fuse.MountOptions{
			FsName: filesystem.Name(),
			Name:   "xgfs",
		},
	})
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Unmount()
		case <-done:
		}
	}()
	server.Wait()
	close(done)
	if err := ctx.Err(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

// dirNode represents a directory inode in FUSE space.
type dirNode struct {
	gofuse.Inode
	fsys xfs.Fs
	path string
}

var (
	_ gofuse.NodeLookuper  = (*dirNode)(nil)
	_ gofuse.NodeReaddirer = (*dirNode)(nil)
	_ gofuse.NodeMkdirer   = (*dirNode)(nil)
	_ gofuse.NodeCreater   = (*dirNode)(nil)
	_ gofuse.NodeUnlinker  = (*dirNode)(nil)
	_ gofuse.NodeRmdirer   = (*dirNode)(nil)
	_ gofuse.NodeSymlinker = (*dirNode)(nil)
	_ gofuse.NodeLinker    = (*dirNode)(nil)
	_ gofuse.NodeGetattrer = (*dirNode)(nil)
)

func newDirNode(fsys xfs.Fs, p string) *dirNode {
	return &dirNode{fsys: fsys, path: cleanPath(p)}
}

func (d *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	entry, err := findEntry(ctx, d.fsys, d.path, name)
	if err != nil {
		return nil, errnoForError(err)
	}
	childPath := joinPath(d.path, name)
	switch {
	case entry.Dir != nil:
		child := newDirNode(d.fsys, childPath)
		attr := directoryAttr(entry.Dir.Metadata(), childPath)
		fillEntry(out, attr)
		return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFDIR)), 0
	case entry.Object != nil:
		child := newFileNode(d.fsys, childPath)
		meta := entry.Object.Metadata()
		attr := fileAttr(meta, entry.Object.Size(), childPath)
		fillEntry(out, attr)
		return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFREG)), 0
	case entry.Link != nil:
		child := newSymlinkNode(d.fsys, childPath, entry.Link.Target)
		attr := symlinkAttr(child.target, childPath)
		fillEntry(out, attr)
		return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFLNK)), 0
	default:
		return nil, syscall.ENOENT
	}
}

func (d *dirNode) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	entries, err := listEntries(ctx, d.fsys, d.path)
	if err != nil {
		return nil, errnoForError(err)
	}
	dirEntries := make([]fuse.DirEntry, 0, len(entries)+2)
	dirEntries = append(dirEntries, fuse.DirEntry{Name: ".", Mode: fuse.S_IFDIR, Ino: inodeForPath(d.path)})
	parent := parentPath(d.path)
	dirEntries = append(dirEntries, fuse.DirEntry{Name: "..", Mode: fuse.S_IFDIR, Ino: inodeForPath(parent)})
	for _, entry := range entries {
		childPath := joinPath(d.path, entry.Name)
		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: entry.Name,
			Mode: entryMode(entry),
			Ino:  inodeForPath(childPath),
		})
	}
	return gofuse.NewListDirStream(dirEntries), 0
}

func (d *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := joinPath(d.path, name)
	dir, err := d.fsys.Mkdir(ctx, childPath, xfs.MkdirOptions{Mode: mode & 0o777})
	if err != nil {
		return nil, errnoForError(err)
	}
	child := newDirNode(d.fsys, childPath)
	attr := directoryAttr(dir.Metadata(), childPath)
	fillEntry(out, attr)
	return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFDIR)), 0
}

func (d *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	childPath := joinPath(d.path, name)
	obj, err := d.fsys.Create(ctx, childPath, xfs.CreateOptions{
		Mode:      mode & 0o777,
		Overwrite: flags&uint32(os.O_TRUNC) != 0,
	})
	if err != nil {
		return nil, nil, 0, errnoForError(err)
	}
	child := newFileNode(d.fsys, childPath)
	attr := fileAttr(obj.Metadata(), obj.Size(), childPath)
	fillEntry(out, attr)
	return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFREG)), nil, 0, 0
}

func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := joinPath(d.path, name)
	if err := d.fsys.Remove(ctx, childPath, xfs.RemoveOptions{}); err != nil {
		return errnoForError(err)
	}
	return 0
}

func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := joinPath(d.path, name)
	if err := d.fsys.Remove(ctx, childPath, xfs.RemoveOptions{}); err != nil {
		return errnoForError(err)
	}
	return 0
}

func (d *dirNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := joinPath(d.path, name)
	if err := d.fsys.Link(ctx, target, childPath, xfs.LinkSymbolic); err != nil {
		return nil, errnoForError(err)
	}
	child := newSymlinkNode(d.fsys, childPath, target)
	attr := symlinkAttr(target, childPath)
	fillEntry(out, attr)
	return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFLNK)), 0
}

func (d *dirNode) Link(ctx context.Context, target gofuse.InodeEmbedder, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := joinPath(d.path, name)
	fileTarget, ok := target.(*fileNode)
	if !ok {
		return nil, syscall.ENOTSUP
	}
	if err := d.fsys.Link(ctx, fileTarget.path, childPath, xfs.LinkHard); err != nil {
		return nil, errnoForError(err)
	}
	obj, err := d.fsys.Stat(ctx, childPath)
	if err != nil {
		return nil, errnoForError(err)
	}
	child := newFileNode(d.fsys, childPath)
	attr := fileAttr(obj.Metadata(), obj.Size(), childPath)
	fillEntry(out, attr)
	return d.NewInode(ctx, child, stableAttr(childPath, fuse.S_IFREG)), 0
}

func (d *dirNode) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := directoryMetadata(ctx, d.fsys, d.path)
	if err != nil {
		return errnoForError(err)
	}
	attr := directoryAttr(meta, d.path)
	fillAttrOut(out, attr)
	return 0
}

// fileNode exposes file semantics.
type fileNode struct {
	gofuse.Inode
	fsys xfs.Fs
	path string
}

var (
	_ gofuse.NodeOpener    = (*fileNode)(nil)
	_ gofuse.NodeReader    = (*fileNode)(nil)
	_ gofuse.NodeWriter    = (*fileNode)(nil)
	_ gofuse.NodeGetattrer = (*fileNode)(nil)
	_ gofuse.NodeSetattrer = (*fileNode)(nil)
)

func newFileNode(fsys xfs.Fs, p string) *fileNode {
	return &fileNode{fsys: fsys, path: cleanPath(p)}
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	obj, err := f.fsys.Stat(ctx, f.path)
	if err != nil {
		return nil, 0, errnoForError(err)
	}
	if flags&uint32(os.O_TRUNC) != 0 {
		if err := obj.Truncate(ctx, 0); err != nil {
			return nil, 0, errnoForError(err)
		}
	}
	return nil, 0, 0
}

func (f *fileNode) Read(ctx context.Context, fh gofuse.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	obj, err := f.fsys.Stat(ctx, f.path)
	if err != nil {
		return nil, errnoForError(err)
	}
	if len(dest) == 0 {
		return fuse.ReadResultData(nil), 0
	}
	buf := make([]byte, len(dest))
	n, readErr := obj.ReadAt(ctx, buf, off, xfs.IOOptions{})
	if readErr != nil && readErr != io.EOF {
		return nil, errnoForError(readErr)
	}
	return fuse.ReadResultData(buf[:n]), 0
}

func (f *fileNode) Write(ctx context.Context, fh gofuse.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	obj, err := f.fsys.Stat(ctx, f.path)
	if err != nil {
		return 0, errnoForError(err)
	}
	payload := append([]byte(nil), data...)
	if _, err := obj.WriteAt(ctx, bytes.NewReader(payload), off, xfs.IOOptions{}); err != nil {
		return 0, errnoForError(err)
	}
	return uint32(len(data)), 0
}

func (f *fileNode) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	obj, err := f.fsys.Stat(ctx, f.path)
	if err != nil {
		return errnoForError(err)
	}
	attr := fileAttr(obj.Metadata(), obj.Size(), f.path)
	fillAttrOut(out, attr)
	return 0
}

func (f *fileNode) Setattr(ctx context.Context, fh gofuse.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		obj, err := f.fsys.Stat(ctx, f.path)
		if err != nil {
			return errnoForError(err)
		}
		if err := obj.Truncate(ctx, int64(size)); err != nil {
			return errnoForError(err)
		}
	}
	return f.Getattr(ctx, fh, out)
}

// symlinkNode implements readlink + getattr for symbolic links.
type symlinkNode struct {
	gofuse.Inode
	fsys   xfs.Fs
	path   string
	target string
}

var (
	_ gofuse.NodeReadlinker = (*symlinkNode)(nil)
	_ gofuse.NodeGetattrer  = (*symlinkNode)(nil)
)

func newSymlinkNode(fsys xfs.Fs, p, target string) *symlinkNode {
	return &symlinkNode{fsys: fsys, path: cleanPath(p), target: target}
}

func (l *symlinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(l.target), 0
}

func (l *symlinkNode) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	attr := symlinkAttr(l.target, l.path)
	fillAttrOut(out, attr)
	return 0
}

// Helper functions.

func joinPath(base, name string) string {
	if base == "/" {
		return cleanPath("/" + name)
	}
	return cleanPath(stdpath.Join(base, name))
}

func parentPath(p string) string {
	if p == "/" {
		return "/"
	}
	parent := stdpath.Dir(p)
	if parent == "" {
		return "/"
	}
	return cleanPath(parent)
}

func entryMode(entry xfs.Entry) uint32 {
	switch {
	case entry.Dir != nil:
		return fuse.S_IFDIR
	case entry.Object != nil:
		return fuse.S_IFREG
	case entry.Link != nil:
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

func listEntries(ctx context.Context, fsys xfs.Fs, dirPath string) ([]xfs.Entry, error) {
	ch, err := fsys.List(ctx, dirPath, xfs.ListOptions{})
	if err != nil {
		return nil, err
	}
	var entries []xfs.Entry
	for entry := range ch {
		entries = append(entries, entry)
	}
	return entries, nil
}

func findEntry(ctx context.Context, fsys xfs.Fs, dirPath, name string) (*xfs.Entry, error) {
	ch, err := fsys.List(ctx, dirPath, xfs.ListOptions{})
	if err != nil {
		return nil, err
	}
	for entry := range ch {
		if entry.Name == name {
			copy := entry
			return &copy, nil
		}
	}
	return nil, xfs.ErrNotFound
}

func directoryMetadata(ctx context.Context, fsys xfs.Fs, p string) (xfs.Metadata, error) {
	if p == "/" {
		root, err := fsys.Root(ctx)
		if err != nil {
			return xfs.Metadata{}, err
		}
		return root.Metadata(), nil
	}
	parent := parentPath(p)
	name := stdpath.Base(p)
	entry, err := findEntry(ctx, fsys, parent, name)
	if err != nil {
		return xfs.Metadata{}, err
	}
	if entry.Dir == nil {
		return xfs.Metadata{}, xfs.ErrNotFound
	}
	return entry.Dir.Metadata(), nil
}

func directoryAttr(meta xfs.Metadata, path string) fuse.Attr {
	return makeAttr(meta, 0, path, fuse.S_IFDIR, defaultDirMod)
}

func fileAttr(meta xfs.Metadata, size int64, path string) fuse.Attr {
	return makeAttr(meta, size, path, fuse.S_IFREG, defaultFileMod)
}

func symlinkAttr(target, path string) fuse.Attr {
	meta := xfs.Metadata{Mode: defaultLinkMod}
	size := int64(len(target))
	return makeAttr(meta, size, path, fuse.S_IFLNK, defaultLinkMod)
}

func makeAttr(meta xfs.Metadata, size int64, path string, typ uint32, fallbackMode uint32) fuse.Attr {
	mode := meta.Mode & 0o777
	if mode == 0 {
		mode = fallbackMode
	}
	attr := fuse.Attr{
		Ino:     inodeForPath(path),
		Mode:    typ | mode,
		Size:    uint64(max64(size, 0)),
		Blocks:  (uint64(max64(size, 0)) + 511) / 512,
		Blksize: defaultBlkSz,
		Nlink:   1,
		Owner: fuse.Owner{
			Uid: meta.UID,
			Gid: meta.GID,
		},
	}
	setTimes(&attr, meta)
	return attr
}

func setTimes(attr *fuse.Attr, meta xfs.Metadata) {
	now := time.Now()
	if meta.MTime.IsZero() {
		attr.Mtime = uint64(now.Unix())
		attr.Mtimensec = uint32(now.Nanosecond())
	} else {
		attr.Mtime = uint64(meta.MTime.Unix())
		attr.Mtimensec = uint32(meta.MTime.Nanosecond())
	}
	if meta.CTime.IsZero() {
		attr.Ctime = attr.Mtime
		attr.Ctimensec = attr.Mtimensec
	} else {
		attr.Ctime = uint64(meta.CTime.Unix())
		attr.Ctimensec = uint32(meta.CTime.Nanosecond())
	}
	attr.Atime = attr.Mtime
	attr.Atimensec = attr.Mtimensec
}

func fillEntry(out *fuse.EntryOut, attr fuse.Attr) {
	out.NodeId = attr.Ino
	out.Attr = attr
	out.SetEntryTimeout(entryTimeout)
	out.SetAttrTimeout(attrTimeout)
}

func fillAttrOut(out *fuse.AttrOut, attr fuse.Attr) {
	out.Attr = attr
	out.SetTimeout(attrTimeout)
}

func stableAttr(path string, typ uint32) gofuse.StableAttr {
	return gofuse.StableAttr{
		Mode: typ,
		Ino:  inodeForPath(path),
	}
}

func inodeForPath(p string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	ino := h.Sum64()
	if ino == 0 {
		return 1
	}
	return ino
}

func max64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

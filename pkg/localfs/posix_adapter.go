package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/meta"
	"github.com/jacktea/xgfs/pkg/vfs"
)

type posixAdapter struct {
	fs *LocalFs
}

var (
	_ vfs.PosixFs       = (*posixAdapter)(nil)
	_ vfs.PosixProvider = (*LocalFs)(nil)
)

// PosixAdapter returns a view of LocalFs satisfying vfs.PosixFs. The adapter keeps
// state inside the underlying LocalFs instance so callers can share locks/caches.
func (l *LocalFs) PosixAdapter() vfs.PosixFs {
	return &posixAdapter{fs: l}
}

func (p *posixAdapter) backend() *LocalFs { return p.fs }

func (p *posixAdapter) OpenFile(ctx context.Context, path string, flags vfs.OpenFlags, perm os.FileMode, user vfs.User) (fs.Object, error) {
	l := p.backend()
	target := cleanPath(path)
	create := flags&vfs.OpenFlagCreate != 0
	write := flags&(vfs.OpenFlagWriteOnly|vfs.OpenFlagReadWrite) != 0
	exclusive := flags&vfs.OpenFlagExclusive != 0
	truncate := flags&vfs.OpenFlagTruncate != 0

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

func (p *posixAdapter) Rename(ctx context.Context, oldPath, newPath string, opts vfs.RenameOptions, user vfs.User) error {
	l := p.backend()
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

func (p *posixAdapter) SetAttr(ctx context.Context, path string, changes vfs.AttrChanges, user vfs.User) error {
	l := p.backend()
	target := cleanPath(path)
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

func (p *posixAdapter) Access(ctx context.Context, path string, mode vfs.AccessMode, user vfs.User) error {
	l := p.backend()
	if _, err := l.resolve(ctx, path); err != nil {
		return err
	}
	// TODO: enforce POSIX permission checks using user identity.
	return nil
}

func (p *posixAdapter) Mknod(ctx context.Context, path string, kind vfs.SpecialKind, perm os.FileMode, dev vfs.DeviceNumber, user vfs.User) error {
	return fs.ErrNotSupported
}

func (p *posixAdapter) Mkfifo(ctx context.Context, path string, perm os.FileMode, user vfs.User) error {
	l := p.backend()
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
		UID:       user.UID,
		GID:       user.GID,
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

func (p *posixAdapter) Symlink(ctx context.Context, target, link string, user vfs.User) error {
	l := p.backend()
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

func (p *posixAdapter) Readlink(ctx context.Context, link string, user vfs.User) (string, error) {
	l := p.backend()
	inode, err := l.resolve(ctx, link)
	if err != nil {
		return "", err
	}
	if inode.Type != meta.TypeSymlink {
		return "", fs.ErrNotSupported
	}
	return inode.Target, nil
}

func (p *posixAdapter) Lock(ctx context.Context, path string, opts vfs.LockOptions, user vfs.User) error {
	l := p.backend()
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

func (p *posixAdapter) Unlock(ctx context.Context, path string, opts vfs.LockOptions, user vfs.User) error {
	l := p.backend()
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

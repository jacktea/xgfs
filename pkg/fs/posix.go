package fs

import (
	"context"
	"io"
	"os"
	"time"
)

// OpenFlags enumerates POSIX open(2) flags.
type OpenFlags uint32

const (
	OpenFlagReadOnly  OpenFlags = 0
	OpenFlagWriteOnly OpenFlags = 1 << iota
	OpenFlagReadWrite
	OpenFlagAppend
	OpenFlagCreate
	OpenFlagExclusive
	OpenFlagTruncate
	OpenFlagSync
)

// AccessMode models F_OK/R_OK/W_OK/X_OK.
type AccessMode uint32

const (
	AccessExists AccessMode = 1 << iota
	AccessRead
	AccessWrite
	AccessExec
)

// SpecialKind describes non-regular file types.
type SpecialKind int

const (
	SpecialNone SpecialKind = iota
	SpecialCharDevice
	SpecialBlockDevice
	SpecialFIFO
	SpecialSocket
)

// DeviceNumber identifies a special file's device major/minor numbers.
type DeviceNumber struct {
	Major uint32
	Minor uint32
}

// User represents the caller identity.
type User struct {
	UID           uint32
	GID           uint32
	Supplementary []uint32
}

// AttrChanges encapsulates chmod/chown/utimes/xattr updates.
type AttrChanges struct {
	Mode   *os.FileMode
	UID    *uint32
	GID    *uint32
	ATime  *time.Time
	MTime  *time.Time
	CTime  *time.Time
	Size   *int64
	XAttrs map[string][]byte
}

// RenameOptions controls rename(2) semantics.
type RenameOptions struct {
	Overwrite bool
	NoReplace bool
	Exchange  bool
}

// LockOptions models advisory locks.
type LockOptions struct {
	Exclusive bool
	Blocking  bool
	Owner     string
	Range     FileRange
}

// FileRange describes a byte-range lock.
type FileRange struct {
	Start  int64
	Length int64 // 0 == to EOF
}

// PosixObject extends Object with write-at/lock semantics.
type PosixObject interface {
	Object
	WriteAt(ctx context.Context, r io.Reader, offset int64, opts IOOptions) (int64, error)
	Truncate(ctx context.Context, size int64) error
}

// PosixFs extends Fs with POSIX operations.
type PosixFs interface {
	Fs

	OpenFile(ctx context.Context, path string, flags OpenFlags, perm os.FileMode, user User) (Object, error)
	Rename(ctx context.Context, oldPath, newPath string, opts RenameOptions, user User) error
	SetAttr(ctx context.Context, path string, changes AttrChanges, user User) error
	Access(ctx context.Context, path string, mode AccessMode, user User) error
	Mknod(ctx context.Context, path string, kind SpecialKind, perm os.FileMode, dev DeviceNumber, user User) error
	Mkfifo(ctx context.Context, path string, perm os.FileMode, user User) error
	Symlink(ctx context.Context, target, link string, user User) error
	Readlink(ctx context.Context, link string, user User) (string, error)
	Lock(ctx context.Context, path string, opts LockOptions, user User) error
	Unlock(ctx context.Context, path string, opts LockOptions, user User) error
}

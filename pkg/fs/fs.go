package fs

import (
	"context"
	"io"
	"time"
)

// ID identifies any inode-backed entity.
type ID string

// Fs is the top-level filesystem contract implemented by every backend.
type Fs interface {
	Name() string
	Features() Features

	Root(ctx context.Context) (Directory, error)
	Stat(ctx context.Context, path string) (Object, error)
	Create(ctx context.Context, path string, opts CreateOptions) (Object, error)
	Mkdir(ctx context.Context, path string, opts MkdirOptions) (Directory, error)
	Remove(ctx context.Context, path string, opts RemoveOptions) error
	List(ctx context.Context, path string, opts ListOptions) (<-chan Entry, error)

	Link(ctx context.Context, source, target string, kind LinkKind) error
	Copy(ctx context.Context, source, target string, opts CopyOptions) (Object, error)
}

// Object describes a single file plus helper methods for I/O.
type Object interface {
	ID() ID
	Path() string
	Size() int64
	ModTime() time.Time
	Metadata() Metadata

	Read(ctx context.Context, w io.WriterAt, opts IOOptions) (int64, error)
	ReadAt(ctx context.Context, p []byte, offset int64, opts IOOptions) (int, error)
	Write(ctx context.Context, r io.Reader, opts IOOptions) (int64, error)
	WriteAt(ctx context.Context, r io.Reader, offset int64, opts IOOptions) (int64, error)
	Truncate(ctx context.Context, size int64) error
	Flush(ctx context.Context) error
}

// Directory represents a single inode directory entry.
type Directory interface {
	ID() ID
	Path() string
	Metadata() Metadata

	Entries(ctx context.Context, opts ListOptions) (<-chan Entry, error)
	CreateFile(ctx context.Context, name string, opts CreateOptions) (Object, error)
	Mkdir(ctx context.Context, name string, opts MkdirOptions) (Directory, error)
	Remove(ctx context.Context, name string, opts RemoveOptions) error
}

// Entry is a unified structure returned when listing directories.
type Entry struct {
	Name   string
	Object Object
	Dir    Directory
	Link   *Link
}

// Metadata captures cross-backend metadata.
type Metadata struct {
	Mode   uint32
	UID    uint32
	GID    uint32
	MTime  time.Time
	CTime  time.Time
	Extras map[string]string
}

// CreateOptions controls object creation.
type CreateOptions struct {
	Mode      uint32
	Overwrite bool
	Metadata  map[string]string
}

// MkdirOptions customises directory creation.
type MkdirOptions struct {
	Parents  bool
	Mode     uint32
	Metadata map[string]string
}

// CopyOptions controls copy-on-write cloning semantics.
type CopyOptions struct {
	Overwrite bool
	Mode      *uint32
	Metadata  map[string]string
}

// RemoveOptions describes deletion semantics.
type RemoveOptions struct {
	Recursive bool
	Force     bool
}

// ListOptions configures listing behavior.
type ListOptions struct {
	Recursive     bool
	Limit         int
	IncludeHidden bool
	StartAfter    string
}

// IOOptions controls streaming operations.
type IOOptions struct {
	ShardSize   int64
	Concurrency int
	UseCache    bool
}

// LinkKind indicates whether a link is hard or symbolic.
type LinkKind int

const (
	LinkHard LinkKind = iota
	LinkSymbolic
)

// Link describes symbolic or hard links.
type Link struct {
	Kind    LinkKind
	Target  string
	InodeID ID
}

// Features advertises backend capabilities.
type Features struct {
	SupportsHardlinks   bool
	SupportsSymlinks    bool
	SupportsCopyOnWrite bool
	SupportsMultipart   bool
}

// Errors returned by Fs implementations.
var (
	ErrNotFound     = Err("not found")
	ErrAlreadyExist = Err("already exists")
	ErrNotSupported = Err("not supported")
)

// Err is a sentinel error type so callers can check via errors.Is.
type Err string

func (e Err) Error() string { return string(e) }

// Driver is a factory method for plugging in new backends.
type Driver func(ctx context.Context, cfg map[string]any) (Fs, error)

var drivers = map[string]Driver{}

// Register installs a backend driver.
func Register(name string, drv Driver) {
	drivers[name] = drv
}

// Open instantiates a driver by name.
func Open(ctx context.Context, name string, cfg map[string]any) (Fs, error) {
	drv, ok := drivers[name]
	if !ok {
		return nil, ErrNotSupported
	}
	return drv(ctx, cfg)
}

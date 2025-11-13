package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"

	xfs "github.com/jacktea/xgfs/pkg/fs"
)

// cleanPath normalises mount-relative paths.
func cleanPath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if cleaned == "" {
		return "/"
	}
	return cleaned
}

// writerAtBuffer collects bytes written through the WriterAt interface.
type writerAtBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *writerAtBuffer) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}
	if int64(int(off)) != off {
		return 0, fmt.Errorf("offset %d too large", off)
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

// Bytes returns the underlying buffer contents.
func (b *writerAtBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf
}

// errnoForError converts repo errors to syscall errno codes.
func errnoForError(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, context.Canceled):
		return syscall.EINTR
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	case errors.Is(err, xfs.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, xfs.ErrAlreadyExist):
		return syscall.EEXIST
	case errors.Is(err, xfs.ErrNotSupported):
		return syscall.ENOTSUP
	case os.IsNotExist(err):
		return syscall.ENOENT
	case os.IsPermission(err):
		return syscall.EPERM
	default:
		return syscall.EIO
	}
}

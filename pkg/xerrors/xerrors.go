package xerrors

import (
	"errors"
	iofs "io/fs"
	"os"

	pkgfs "github.com/jacktea/xgfs/pkg/fs"
)

// Kind classifies xgfs errors.
type Kind int

const (
	KindInvalid Kind = iota
	KindNotFound
	KindAlreadyExists
	KindPermission
	KindRange
	KindNotSupported
	KindInternal
)

// Error wraps an underlying error with additional metadata.
type Error struct {
	Kind Kind
	Op   string
	Path string
	Err  error
}

// Error implements the error interface.
func (e *Error) Error() string {
	base := kindString(e.Kind)
	if e.Op != "" {
		base = e.Op + ": " + base
	}
	if e.Path != "" {
		base += " " + e.Path
	}
	if e.Err != nil {
		return base + ": " + e.Err.Error()
	}
	return base
}

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error { return e.Err }

func kindString(kind Kind) string {
	switch kind {
	case KindNotFound:
		return "not found"
	case KindAlreadyExists:
		return "already exists"
	case KindPermission:
		return "permission denied"
	case KindRange:
		return "invalid range"
	case KindNotSupported:
		return "not supported"
	case KindInternal:
		return "internal error"
	default:
		return "invalid"
	}
}

// Wrap annotates err with the given metadata. If err is nil, Wrap returns nil.
func Wrap(kind Kind, op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Op: op, Path: path, Err: err}
}

// E creates a new error with the provided metadata (no underlying error).
func E(kind Kind, op, path string) error {
	return &Error{Kind: kind, Op: op, Path: path}
}

// KindOf extracts the Kind from err, walking wrapped errors as needed.
func KindOf(err error) Kind {
	if err == nil {
		return KindInvalid
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	switch {
	case errors.Is(err, pkgfs.ErrNotFound),
		errors.Is(err, iofs.ErrNotExist),
		errors.Is(err, os.ErrNotExist):
		return KindNotFound
	case errors.Is(err, pkgfs.ErrAlreadyExist),
		errors.Is(err, iofs.ErrExist),
		errors.Is(err, os.ErrExist):
		return KindAlreadyExists
	case errors.Is(err, pkgfs.ErrNotSupported):
		return KindNotSupported
	case errors.Is(err, iofs.ErrPermission),
		errors.Is(err, os.ErrPermission):
		return KindPermission
	case errors.Is(err, iofs.ErrInvalid):
		return KindInvalid
	default:
		return KindInternal
	}
}

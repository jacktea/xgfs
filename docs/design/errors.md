# Error Handling Design

This document defines the shared error model for `github.com/jacktea/xgfs`. The goals are:

1. Provide consistent categories so callers (HTTP/NFS/S3/CLI) can convert internal failures into user-facing status codes/messages.
2. Preserve context (operation, path, wrapped error) while staying compatible with Go's `errors.Is/As`.
3. Allow low-level packages (blob/meta/localfs) to keep using standard library errors (e.g. `os.ErrPermission`) when appropriate.

## Error Categories

All repository-defined errors fall into the following `Kind`s:

| Kind | Description | HTTP default | CLI default |
|------|-------------|--------------|-------------|
| `KindNotFound` | Object/path missing (`fs.ErrNotFound`, 404) | `404 Not Found` | `not found` |
| `KindAlreadyExists` | Path exists when creation requested (`fs.ErrAlreadyExist`, 409) | `409 Conflict` | `already exists` |
| `KindPermission` | Permission denied / sticky bit violation / ACL failure | `403 Forbidden` | `permission denied` |
| `KindRange` | Invalid byte range / offset (e.g. HTTP Range, shard offset) | `416 Range Not Satisfiable` | `invalid range` |
| `KindInvalid` | Malformed request (bad path, JSON, flag) | `400 Bad Request` | descriptive message |
| `KindNotSupported` | Feature unsupported on backend | `501 Not Implemented` | `operation not supported` |
| `KindInternal` | Unexpected internal error (default catch-all) | `500 Internal Server Error` | `internal error` |

Kind values will live in `pkg/xerrors`, see below. `fs.ErrNotSupported`/`fs.ErrNotFound` remain to preserve compatibility; `xerrors.KindNotFound` will wrap the new sentinel.

## Standard Library Integration

When operations fail due to underlying OS or standard library conditions, we continue to return those errors directly:

- `os.ErrPermission`, `os.ErrExist`, `os.ErrNotExist`
- `io.ErrUnexpectedEOF`, `context.Canceled`, `context.DeadlineExceeded`

Callers should use `errors.Is` to check for these in addition to the new kinds. `xerrors.Error` will wrap underlying errors so `errors.Is(err, os.ErrPermission)` still works.

## `pkg/xerrors`

Add a package with the following surface:

```go
package xerrors

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

type Error struct {
    Kind Kind
    Op   string // optional: operation name ("localfs.Mkdir")
    Path string // optional: path related to the error
    Err  error  // underlying error
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
```

Helpers:

- `func Wrap(kind Kind, op, path string, err error) error`
- `func E(kind Kind, op, path string) error` for leaf errors
- `func KindOf(err error) Kind`

Implementation MUST use `errors.As` internally so that both `Kind` checks and standard errors are available.

## Usage Guidelines

1. **Low-level packages**:
   - When returning sentinel errors (`fs.ErrNotFound`), keep returning them, but also wrap via `xerrors.Wrap(KindNotFound, op, path, fs.ErrNotFound)` so callers have both signals.
   - Preserve the original `err` when adding context.

2. **VFS / LocalFS**:
   - All permission failures should return `xerrors.KindPermission` (wrapping `os.ErrPermission` when relevant).
   - Range/offset validation should return `KindRange`.

3. **HTTP/S3/NFS/CLI**:
   - Use `xerrors.KindOf(err)` to map to status codes listed above.
   - For unknown kinds, fall back to 500/"internal error" but log the wrapped error.

4. **CLI**:
   - Print user-friendly messages per kind (e.g. `fmt.Fprintf(os.Stderr, "xgfs: permission denied: %v\n", err)`).

## Future Work

- Evaluate adding `KindTimeout` if we expose retryable timeouts.
- Optionally add structured logging fields (e.g., `map[string]string`) to `xerrors.Error` once logging infrastructure is in place.

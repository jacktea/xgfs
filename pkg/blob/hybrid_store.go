package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// HybridOptions control hybrid store behaviour.
type HybridOptions struct {
	MirrorSecondary bool // if true, writes are mirrored to secondary
	CacheOnRead     bool // if true, cache remote reads into primary
}

// HybridStore layers a primary (usually local) store with a secondary backend.
type HybridStore struct {
	primary   Store
	secondary Store
	opts      HybridOptions
}

// NewHybridStore composes primary and secondary blob stores.
func NewHybridStore(primary Store, secondary Store, opts HybridOptions) (*HybridStore, error) {
	if primary == nil {
		return nil, fmt.Errorf("hybrid: primary store required")
	}
	if secondary == nil {
		return nil, fmt.Errorf("hybrid: secondary store required")
	}
	return &HybridStore{primary: primary, secondary: secondary, opts: opts}, nil
}

func (h *HybridStore) Put(ctx context.Context, r io.Reader, size int64, opts PutOptions) (ID, int64, error) {
	if h.secondary == nil {
		return h.primary.Put(ctx, r, size, opts)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	reader := func() io.ReadSeeker { return bytes.NewReader(data) }
	id, written, err := h.primary.Put(ctx, reader(), int64(len(data)), opts)
	if err != nil {
		return "", 0, err
	}
	if h.opts.MirrorSecondary {
		if _, _, err := h.secondary.Put(ctx, reader(), int64(len(data)), opts); err != nil {
			return "", 0, err
		}
	}
	_ = size
	return id, written, nil
}

func (h *HybridStore) Get(ctx context.Context, id ID) (io.ReadCloser, int64, error) {
	rc, size, err := h.primary.Get(ctx, id)
	if err == nil {
		return rc, size, nil
	}
	if h.secondary == nil {
		return nil, 0, err
	}
	rc2, _, err := h.secondary.Get(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(rc2)
	rc2.Close()
	if err != nil {
		return nil, 0, err
	}
	if h.opts.CacheOnRead {
		_, _, _ = h.primary.Put(ctx, bytes.NewReader(data), int64(len(data)), PutOptions{})
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (h *HybridStore) Delete(ctx context.Context, id ID) error {
	var primaryErr error
	if err := h.primary.Delete(ctx, id); err != nil {
		primaryErr = err
	}
	if h.secondary != nil {
		if err := h.secondary.Delete(ctx, id); err != nil && primaryErr == nil {
			primaryErr = err
		}
	}
	return primaryErr
}

func (h *HybridStore) Exists(ctx context.Context, id ID) (bool, error) {
	ok, err := h.primary.Exists(ctx, id)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	if h.secondary == nil {
		return false, nil
	}
	return h.secondary.Exists(ctx, id)
}

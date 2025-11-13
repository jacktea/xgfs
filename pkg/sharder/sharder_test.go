package sharder

import (
	"bytes"
	"context"
	"testing"

	"github.com/jacktea/xgfs/pkg/blob"
)

func TestChunkAndStoreConcurrentMatchesSequential(t *testing.T) {
	ctx := context.Background()
	store, err := blob.NewPathStore(t.TempDir())
	if err != nil {
		t.Fatalf("path store: %v", err)
	}
	payload := bytes.Repeat([]byte("concurrent-data"), 1<<12)
	shards, err := ChunkAndStore(ctx, store, bytes.NewReader(payload), WriterOptions{
		ChunkSize:   64 << 10,
		Concurrency: 4,
	})
	if err != nil {
		t.Fatalf("chunk store: %v", err)
	}
	writer := newSliceWriter(len(payload))
	if _, err := Concat(ctx, store, shards, writer, ReaderOptions{Concurrency: 4}); err != nil {
		t.Fatalf("concat: %v", err)
	}
	if !bytes.Equal(writer.Bytes(), payload) {
		t.Fatalf("round trip mismatch")
	}
}

type sliceWriter struct {
	buf []byte
}

func newSliceWriter(size int) *sliceWriter {
	return &sliceWriter{buf: make([]byte, size)}
}

func (s *sliceWriter) WriteAt(p []byte, off int64) (int, error) {
	end := int(off) + len(p)
	if end > len(s.buf) {
		expanded := make([]byte, end)
		copy(expanded, s.buf)
		s.buf = expanded
	}
	copy(s.buf[int(off):end], p)
	return len(p), nil
}

func (s *sliceWriter) Bytes() []byte {
	return s.buf
}

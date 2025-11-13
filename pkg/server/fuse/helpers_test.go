package fuse

import (
	"context"
	"syscall"
	"testing"

	xfs "github.com/jacktea/xgfs/pkg/fs"
)

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":        "/",
		"/":       "/",
		"foo/bar": "/foo/bar",
		"/foo//":  "/foo",
	}
	for in, want := range tests {
		if got := cleanPath(in); got != want {
			t.Fatalf("cleanPath(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestWriterAtBuffer(t *testing.T) {
	var buf writerAtBuffer
	if _, err := buf.WriteAt([]byte("world"), 5); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := buf.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := string(buf.Bytes()); got != "helloworld" {
		t.Fatalf("got %q want %q", got, "helloworld")
	}
}

func TestErrnoForError(t *testing.T) {
	if errnoForError(nil) != 0 {
		t.Fatalf("expected 0 for nil")
	}
	if errnoForError(xfs.ErrNotFound) != syscall.ENOENT {
		t.Fatalf("expected ENOENT")
	}
	if errnoForError(context.Canceled) != syscall.EINTR {
		t.Fatalf("expected EINTR")
	}
}

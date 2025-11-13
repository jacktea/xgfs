package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/vfs"
)

func newTestFS(t *testing.T) *filesystem {
	t.Helper()
	ctx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 0})
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	bfs, err := newFilesystem(ctx, vfs.New(backend, vfs.Options{}), "/")
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	return bfs.(*filesystem)
}

func TestFilesystemCreateRead(t *testing.T) {
	fsys := newTestFS(t)
	w, err := fsys.Create("/hello.txt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := w.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	r, err := fsys.Open("/hello.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("unexpected data %q", string(data))
	}
	r.Close()
}

func TestFilesystemReadDir(t *testing.T) {
	fsys := newTestFS(t)
	if _, err := fsys.Create("/a.txt"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if err := fsys.MkdirAll("/d/sub", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	infos, err := fsys.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var names []string
	for _, info := range infos {
		names = append(names, info.Name())
	}
	sort.Strings(names)
	expected := []string{"a.txt", "d"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %v", len(expected), names)
	}
	for i, name := range expected {
		if names[i] != name {
			t.Fatalf("unexpected entry list %v", names)
		}
	}
}

func TestFilesystemSymlink(t *testing.T) {
	fsys := newTestFS(t)
	if _, err := fsys.Create("/a.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsys.Symlink("/a.txt", "/link-a"); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	target, err := fsys.Readlink("/link-a")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "/a.txt" {
		t.Fatalf("expected /a.txt, got %s", target)
	}
}

func TestFilesystemRemoveDir(t *testing.T) {
	fsys := newTestFS(t)
	if err := fsys.MkdirAll("/nested/dir", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := fsys.Remove("/nested"); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if _, err := fsys.Stat("/nested"); !os.IsNotExist(err) {
		t.Fatalf("expected nested removed, got %v", err)
	}
}

func TestFilesystemRenameFile(t *testing.T) {
	fsys := newTestFS(t)
	file, err := fsys.Create("/old.txt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	file.Write([]byte("data"))
	file.Close()
	if err := fsys.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := fsys.Stat("/old.txt"); !os.IsNotExist(err) {
		t.Fatalf("expected old missing, got %v", err)
	}
	info, err := fsys.Stat("/new.txt")
	if err != nil {
		t.Fatalf("stat new: %v", err)
	}
	if info.Size() != 4 {
		t.Fatalf("expected size 4, got %d", info.Size())
	}
}

func TestFilesystemRenameDirectory(t *testing.T) {
	fsys := newTestFS(t)
	if err := fsys.MkdirAll("/dir/sub", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := fsys.Create("/dir/sub/file.txt"); err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := fsys.Rename("/dir", "/dir2"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	if _, err := fsys.back.Stat(fsys.ctx, "/dir2/sub/file.txt"); err != nil {
		t.Fatalf("backend stat new: %v", err)
	}
	if _, err := fsys.back.Stat(fsys.ctx, "/dir/sub/file.txt"); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected backend to miss old path, got %v", err)
	}
	if _, err := fsys.Stat("/dir2/sub/file.txt"); err != nil {
		t.Fatalf("stat renamed file: %v", err)
	}
	if _, err := fsys.Stat("/dir/sub/file.txt"); err == nil {
		t.Fatalf("expected old path missing")
	}
}

func TestCleanPath(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"", "/"},
		{".", "/"},
		{"foo", "/foo"},
		{"/foo/../bar", "/bar"},
	}
	for _, tt := range tests {
		if got := cleanPath(tt.in); got != tt.out {
			t.Fatalf("cleanPath(%q)=%q want %q", tt.in, got, tt.out)
		}
	}
}

func TestJoinRespectsRoot(t *testing.T) {
	fsys := newTestFS(t)
	if err := fsys.MkdirAll("/d", 0o755); err != nil {
		t.Fatalf("mkdir /d: %v", err)
	}
	child, err := fsys.Chroot("/d")
	if err != nil {
		t.Fatalf("chroot: %v", err)
	}
	if err := child.MkdirAll("/sub", 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	fp := child.Join("/sub", "../sub/file.txt")
	expected := path.Clean("/sub/../sub/file.txt")
	if fp != expected {
		t.Fatalf("unexpected join result %s want %s", fp, expected)
	}
}

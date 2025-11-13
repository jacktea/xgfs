package vfs_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/vfs"
)

func TestFSDelegatesToPosixBackend(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	_, err = vfsBackend.OpenFile(ctx, "/file.txt", vfs.OpenFlagCreate|vfs.OpenFlagWriteOnly, 0o644, vfs.User{})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
}

func TestFSReturnsErrWhenBackendLacksPosix(t *testing.T) {
	ctx := context.Background()
	mock := fakeBackend{}
	vfsBackend := vfs.New(mock, vfs.Options{})
	_, err := vfsBackend.OpenFile(ctx, "/file.txt", vfs.OpenFlagReadOnly, 0o644, vfs.User{})
	if !errors.Is(err, fs.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
	if err := vfsBackend.Rename(ctx, "/a", "/b", vfs.RenameOptions{}, vfs.User{}); !errors.Is(err, fs.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported, got %v", err)
	}
}

func TestAccessEnforcesPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Create(ctx, "/secure.txt", fs.CreateOptions{Mode: 0o640, Overwrite: true}); err != nil {
		t.Fatalf("create: %v", err)
	}
	uid := uint32(1000)
	gid := uint32(2000)
	if err := vfsBackend.SetAttr(ctx, "/secure.txt", vfs.AttrChanges{UID: &uid, GID: &gid}, vfs.User{}); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	owner := vfs.User{UID: 1000, GID: 2000}
	if err := vfsBackend.Access(ctx, "/secure.txt", vfs.AccessRead|vfs.AccessWrite, owner); err != nil {
		t.Fatalf("owner access: %v", err)
	}
	groupOnly := vfs.User{UID: 1500, GID: 2000}
	if err := vfsBackend.Access(ctx, "/secure.txt", vfs.AccessRead, groupOnly); err != nil {
		t.Fatalf("group read: %v", err)
	}
	if err := vfsBackend.Access(ctx, "/secure.txt", vfs.AccessWrite, groupOnly); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected group write denied, got %v", err)
	}
	other := vfs.User{UID: 42, GID: 42}
	if err := vfsBackend.Access(ctx, "/secure.txt", vfs.AccessRead, other); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected other read denied, got %v", err)
	}
	root := vfs.User{UID: 0, GID: 0}
	if err := vfsBackend.Access(ctx, "/secure.txt", vfs.AccessWrite, root); err != nil {
		t.Fatalf("root bypass: %v", err)
	}
}

func TestAccessChecksDirectories(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/private", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	uid := uint32(1234)
	gid := uint32(5678)
	if err := vfsBackend.SetAttr(ctx, "/private", vfs.AttrChanges{UID: &uid, GID: &gid}, vfs.User{}); err != nil {
		t.Fatalf("setattr dir: %v", err)
	}
	owner := vfs.User{UID: 1234, GID: 5678}
	if err := vfsBackend.Access(ctx, "/private", vfs.AccessExec, owner); err != nil {
		t.Fatalf("owner exec: %v", err)
	}
	other := vfs.User{UID: 9999, GID: 9999}
	if err := vfsBackend.Access(ctx, "/private", vfs.AccessExec, other); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected directory exec denied, got %v", err)
	}
	if err := vfsBackend.Access(ctx, "/private", vfs.AccessExists, other); err != nil {
		t.Fatalf("AccessExists should only check presence, got %v", err)
	}
}

func TestCreateChecksParentPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/secure", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	ownerID := uint32(1111)
	if err := vfsBackend.SetAttr(ctx, "/secure", vfs.AttrChanges{UID: &ownerID}, vfs.User{}); err != nil {
		t.Fatalf("setattr dir: %v", err)
	}
	ownerCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 1111})
	if _, err := vfsBackend.Create(ownerCtx, "/secure/owned.txt", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("owner create: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 2222})
	if _, err := vfsBackend.Create(otherCtx, "/secure/denied.txt", fs.CreateOptions{Mode: 0o600, Overwrite: true}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected create denied, got %v", err)
	}
}

func TestMkdirChecksParentPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/top", fs.MkdirOptions{Mode: 0o750}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	owner := uint32(123)
	if err := vfsBackend.SetAttr(ctx, "/top", vfs.AttrChanges{UID: &owner}, vfs.User{}); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	ownerCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 123})
	if _, err := vfsBackend.Mkdir(ownerCtx, "/top/child", fs.MkdirOptions{Mode: 0o755}); err != nil {
		t.Fatalf("owner mkdir: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 999})
	if _, err := vfsBackend.Mkdir(otherCtx, "/top/denied", fs.MkdirOptions{Mode: 0o755}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected mkdir denied, got %v", err)
	}
}

func TestRenameChecksDirectoryPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/src", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if _, err := vfsBackend.Mkdir(ctx, "/dst", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	if _, err := vfsBackend.Create(ctx, "/src/file.txt", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("create file: %v", err)
	}
	uid := uint32(2000)
	gid := uint32(3000)
	if err := vfsBackend.SetAttr(ctx, "/src", vfs.AttrChanges{UID: &uid, GID: &gid}, vfs.User{}); err != nil {
		t.Fatalf("setattr src: %v", err)
	}
	if err := vfsBackend.SetAttr(ctx, "/dst", vfs.AttrChanges{UID: &uid, GID: &gid}, vfs.User{}); err != nil {
		t.Fatalf("setattr dst: %v", err)
	}
	owner := vfs.User{UID: 2000, GID: 3000}
	if err := vfsBackend.Rename(ctx, "/src/file.txt", "/dst/file.txt", vfs.RenameOptions{}, owner); err != nil {
		t.Fatalf("owner rename: %v", err)
	}
	other := vfs.User{UID: 4000, GID: 4000}
	if err := vfsBackend.Rename(ctx, "/dst/file.txt", "/src/file2.txt", vfs.RenameOptions{}, other); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected rename denied, got %v", err)
	}
}

func TestSetAttrRequiresOwnership(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Create(ctx, "/file.txt", fs.CreateOptions{Mode: 0o644, Overwrite: true}); err != nil {
		t.Fatalf("create: %v", err)
	}
	uid := uint32(1111)
	if err := vfsBackend.SetAttr(ctx, "/file.txt", vfs.AttrChanges{UID: &uid}, vfs.User{}); err != nil {
		t.Fatalf("set uid: %v", err)
	}
	owner := vfs.User{UID: 1111}
	mode := os.FileMode(0o600)
	if err := vfsBackend.SetAttr(ctx, "/file.txt", vfs.AttrChanges{Mode: &mode}, owner); err != nil {
		t.Fatalf("owner chmod: %v", err)
	}
	other := vfs.User{UID: 2222}
	mode = 0o777
	if err := vfsBackend.SetAttr(ctx, "/file.txt", vfs.AttrChanges{Mode: &mode}, other); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected chmod denied, got %v", err)
	}
}

func TestRemoveChecksParentPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/vault", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	uid := uint32(55)
	if err := vfsBackend.SetAttr(ctx, "/vault", vfs.AttrChanges{UID: &uid}, vfs.User{}); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	ownerCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 55})
	if _, err := vfsBackend.Create(ownerCtx, "/vault/item", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := vfsBackend.Remove(ownerCtx, "/vault/item", fs.RemoveOptions{}); err != nil {
		t.Fatalf("owner remove: %v", err)
	}
	if _, err := vfsBackend.Create(ownerCtx, "/vault/item2", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("create2: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 77})
	if err := vfsBackend.Remove(otherCtx, "/vault/item2", fs.RemoveOptions{}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected remove denied, got %v", err)
	}
}

func TestMkfifoChecksParentPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{
		BlobRoot:     t.TempDir(),
		CacheEntries: 8,
	})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/pipes", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	uid := uint32(42)
	if err := vfsBackend.SetAttr(ctx, "/pipes", vfs.AttrChanges{UID: &uid}, vfs.User{}); err != nil {
		t.Fatalf("setattr: %v", err)
	}
	if err := vfsBackend.Mkfifo(ctx, "/pipes/fifo", 0o600, vfs.User{UID: 42}); err != nil {
		t.Fatalf("owner mkfifo: %v", err)
	}
	if err := vfsBackend.Mkfifo(ctx, "/pipes/fifo2", 0o600, vfs.User{UID: 99}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected mkfifo denied, got %v", err)
	}
}

func TestLinkHonorsPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Create(ctx, "/file.txt", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("create file: %v", err)
	}
	if _, err := vfsBackend.Mkdir(ctx, "/links", fs.MkdirOptions{Mode: 0o700}); err != nil {
		t.Fatalf("mkdir links: %v", err)
	}
	dirOwner := uint32(600)
	if err := vfsBackend.SetAttr(ctx, "/links", vfs.AttrChanges{UID: &dirOwner}, vfs.User{}); err != nil {
		t.Fatalf("setattr links: %v", err)
	}
	fileOwner := uint32(600)
	if err := vfsBackend.SetAttr(ctx, "/file.txt", vfs.AttrChanges{UID: &fileOwner}, vfs.User{}); err != nil {
		t.Fatalf("setattr file: %v", err)
	}
	ownerCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 600})
	if err := vfsBackend.Link(ownerCtx, "/file.txt", "/links/hard", fs.LinkHard); err != nil {
		t.Fatalf("owner hard link: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 700})
	if err := vfsBackend.Link(otherCtx, "/file.txt", "/links/deny", fs.LinkHard); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected hard link denied, got %v", err)
	}
}

func TestSymlinkChecksParentPermissions(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/links", fs.MkdirOptions{Mode: 0o755}); err != nil {
		t.Fatalf("mkdir links: %v", err)
	}
	uid := uint32(800)
	if err := vfsBackend.SetAttr(ctx, "/links", vfs.AttrChanges{UID: &uid}, vfs.User{}); err != nil {
		t.Fatalf("setattr links: %v", err)
	}
	ownerCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 800})
	if err := vfsBackend.Symlink(ownerCtx, "/target", "/links/sym", vfs.User{UID: 800}); err != nil {
		t.Fatalf("owner symlink: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 900})
	if err := vfsBackend.Symlink(otherCtx, "/target", "/links/deny", vfs.User{UID: 900}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected symlink denied, got %v", err)
	}
}

func TestStickyBitRemoval(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	vfsBackend := vfs.New(backend, vfs.Options{})
	if _, err := vfsBackend.Mkdir(ctx, "/sticky", fs.MkdirOptions{Mode: 0o1777}); err != nil {
		t.Fatalf("mkdir sticky: %v", err)
	}
	dirOwner := uint32(111)
	mode := os.FileMode(0o777) | os.ModeSticky
	if err := vfsBackend.SetAttr(ctx, "/sticky", vfs.AttrChanges{UID: &dirOwner, Mode: &mode}, vfs.User{}); err != nil {
		t.Fatalf("setattr sticky: %v", err)
	}
	fileCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 222})
	if _, err := vfsBackend.Create(fileCtx, "/sticky/file", fs.CreateOptions{Mode: 0o600, Overwrite: true}); err != nil {
		t.Fatalf("create file: %v", err)
	}
	fileOwner := uint32(222)
	if err := vfsBackend.SetAttr(ctx, "/sticky/file", vfs.AttrChanges{UID: &fileOwner}, vfs.User{}); err != nil {
		t.Fatalf("setattr file: %v", err)
	}
	otherCtx := vfs.NewContextWithUser(context.Background(), vfs.User{UID: 333})
	if err := vfsBackend.Remove(otherCtx, "/sticky/file", fs.RemoveOptions{}); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("expected sticky delete denied, got %v", err)
	}
	if err := vfsBackend.Remove(fileCtx, "/sticky/file", fs.RemoveOptions{}); err != nil {
		t.Fatalf("owner remove should succeed: %v", err)
	}
}

type fakeBackend struct{}

func (fakeBackend) Name() string                                    { return "fake" }
func (fakeBackend) Features() fs.Features                           { return fs.Features{} }
func (fakeBackend) Root(context.Context) (fs.Directory, error)      { return nil, fs.ErrNotSupported }
func (fakeBackend) Stat(context.Context, string) (fs.Object, error) { return nil, fs.ErrNotSupported }
func (fakeBackend) Create(context.Context, string, fs.CreateOptions) (fs.Object, error) {
	return nil, fs.ErrNotSupported
}
func (fakeBackend) Mkdir(context.Context, string, fs.MkdirOptions) (fs.Directory, error) {
	return nil, fs.ErrNotSupported
}
func (fakeBackend) Remove(context.Context, string, fs.RemoveOptions) error { return fs.ErrNotSupported }
func (fakeBackend) List(context.Context, string, fs.ListOptions) (<-chan fs.Entry, error) {
	return nil, fs.ErrNotSupported
}
func (fakeBackend) Link(context.Context, string, string, fs.LinkKind) error {
	return fs.ErrNotSupported
}
func (fakeBackend) Copy(context.Context, string, string, fs.CopyOptions) (fs.Object, error) {
	return nil, fs.ErrNotSupported
}

// Ensure fakeBackend satisfies fs.Fs so the compiler enforces coverage.
var _ fs.Fs = (*fakeBackend)(nil)

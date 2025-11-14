package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/jacktea/xgfs/pkg/encryption"
	"github.com/jacktea/xgfs/pkg/xerrors"
)

// PathStore persists shards on the local filesystem.
type PathStore struct {
	root string
}

// NewPathStore returns a Store rooted at path.
func NewPathStore(root string) (*PathStore, error) {
	if root == "" {
		return nil, xerrors.E(xerrors.KindInvalid, "PathStore", "root")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, xerrors.Wrap(xerrors.KindInternal, "PathStore.mkdir", root, err)
	}
	return &PathStore{root: root}, nil
}

func (p *PathStore) Put(ctx context.Context, r io.Reader, size int64, opts PutOptions) (ID, int64, error) {
	hasher := sha256.New()
	tee := io.TeeReader(r, hasher)
	file, err := os.CreateTemp(p.root, "upload-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := file.Name()
	var writer io.Writer = file
	var bytesWritten int64
	if opts.Encryption.Enabled() {
		wrapped, overhead, err := encryption.WrapWriter(file, opts.Encryption)
		if err != nil {
			file.Close()
			os.Remove(tmpName)
			return "", 0, err
		}
		writer = wrapped
		bytesWritten += overhead
	}
	n, err := io.Copy(writer, tee)
	if err != nil {
		return "", 0, err
	}
	bytesWritten += n
	sum := hasher.Sum(nil)
	blade := hex.EncodeToString(sum)
	finalPath := p.pathForID(ID(blade))
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpName)
		return "", 0, err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpName)
		return "", 0, err
	}
	if _, err := os.Stat(finalPath); err == nil {
		os.Remove(tmpName)
		return ID(blade), bytesWritten, nil
	} else if !os.IsNotExist(err) {
		os.Remove(tmpName)
		return "", 0, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		os.Remove(tmpName)
		return "", 0, err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		os.Remove(tmpName)
		return "", 0, err
	}
	return ID(blade), bytesWritten, nil
}

func (p *PathStore) Get(ctx context.Context, id ID) (io.ReadCloser, int64, error) {
	path := p.pathForID(id)
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

func (p *PathStore) Delete(ctx context.Context, id ID) error {
	path := p.pathForID(id)
	return os.Remove(path)
}

func (p *PathStore) Exists(ctx context.Context, id ID) (bool, error) {
	path := p.pathForID(id)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (p *PathStore) pathForID(id ID) string {
	name := string(id)
	if len(name) < 4 {
		return filepath.Join(p.root, name)
	}
	return filepath.Join(p.root, name[:2], name[2:4], name)
}

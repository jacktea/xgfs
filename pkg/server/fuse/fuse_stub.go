//go:build !linux

package fuse

import (
	"context"
	"fmt"

	xfs "github.com/jacktea/xgfs/pkg/fs"
)

// Mount wires the repository fs.Fs into a FUSE mountpoint.
func Mount(ctx context.Context, filesystem xfs.Fs, mountpoint string) error {
	return fmt.Errorf("fuse mount not supported in this build")
}

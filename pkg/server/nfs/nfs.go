package nfs

import (
	"context"
	"fmt"
	"net"
	"strings"

	billy "github.com/go-git/go-billy/v5"
	nfsproto "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/jacktea/xgfs/pkg/fs"
)

// Options control the exported NFS service.
type Options struct {
	// Export is the root path presented to clients (default "/").
	Export string
	// HandleCache controls how many active file handles are cached (default 1024).
	HandleCache int
}

// Serve exposes filesystem over NFS at addr using default options.
func Serve(ctx context.Context, filesystem fs.Fs, addr string) error {
	return ServeWithOptions(ctx, filesystem, addr, Options{})
}

// ServeWithOptions exposes filesystem over NFS with custom options.
func ServeWithOptions(ctx context.Context, filesystem fs.Fs, addr string, opts Options) error {
	if filesystem == nil {
		return fmt.Errorf("nfs: filesystem is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if addr == "" {
		addr = ":2049"
	}
	export := strings.TrimSpace(opts.Export)
	if export == "" {
		export = "/"
	}
	cacheSize := opts.HandleCache
	if cacheSize <= 0 {
		cacheSize = 1024
	}
	bfs, err := newFilesystem(ctx, filesystem, export)
	if err != nil {
		return fmt.Errorf("nfs: %w", err)
	}
	handler := nfshelper.NewNullAuthHandler(bfs)
	handler = nfshelper.NewCachingHandler(handler, cacheSize)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("nfs: listen: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	srv := &nfsproto.Server{
		Handler: handler,
		Context: ctx,
	}
	return srv.Serve(l)
}

var _ billy.Filesystem = (*filesystem)(nil)

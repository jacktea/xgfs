package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/server/fuse"
	"github.com/jacktea/xgfs/pkg/server/httpapi"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/server/nfs"
	"github.com/jacktea/xgfs/pkg/server/s3gw"
)

func main() {
	var (
		blobRoot      = flag.String("root", ".xgfs/blobs", "blob storage root (local provider)")
		chunkMB       = flag.Int("chunk", 4, "shard size in MiB")
		encrypt       = flag.Bool("encrypt", false, "enable shard encryption")
		keyHex        = flag.String("key", "", "hex-encoded 32-byte key when encryption enabled")
		metaPath      = flag.String("meta", "", "path to metadata file store")
		storageProv   = flag.String("storage-provider", "local", "storage provider: local|s3|oss|cos")
		storageEP     = flag.String("storage-endpoint", "", "remote storage endpoint")
		storageBucket = flag.String("storage-bucket", "", "remote storage bucket name")
		storageRegion = flag.String("storage-region", "", "region (S3 only)")
		storageAK     = flag.String("storage-access-key", "", "remote storage access key")
		storageSK     = flag.String("storage-secret-key", "", "remote storage secret key")
		storageToken  = flag.String("storage-session-token", "", "remote storage session token (S3)")
		hybridProv    = flag.String("hybrid-provider", "", "secondary storage provider for hybrid tier")
		hybridEP      = flag.String("hybrid-endpoint", "", "secondary storage endpoint")
		hybridBucket  = flag.String("hybrid-bucket", "", "secondary storage bucket")
		hybridRegion  = flag.String("hybrid-region", "", "secondary storage region (S3 only)")
		hybridAK      = flag.String("hybrid-access-key", "", "secondary storage access key")
		hybridSK      = flag.String("hybrid-secret-key", "", "secondary storage secret key")
		hybridToken   = flag.String("hybrid-session-token", "", "secondary storage session token (S3)")
		hybridMirror  = flag.Bool("hybrid-mirror", true, "mirror writes to the secondary store")
		hybridCache   = flag.Bool("hybrid-cache-read", true, "cache secondary reads into the primary store")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: xgfs [flags] <command> [args]\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Commands: ls, put, cat, cp, chmod, chown, rename, mkfifo, gc, serve-* ...\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	key := []byte(nil)
	if *encrypt {
		if len(*keyHex) == 0 {
			fmt.Fprintln(os.Stderr, "encryption enabled but --key missing")
			os.Exit(1)
		}
		decoded, err := hex.DecodeString(*keyHex)
		if err != nil || len(decoded) != 32 {
			fmt.Fprintln(os.Stderr, "--key must be 32 bytes of hex")
			os.Exit(1)
		}
		key = decoded
	}
	ctx := context.Background()
	blobStore, err := buildBlobStore(*storageProv, storageOptions{
		Endpoint:     *storageEP,
		Bucket:       *storageBucket,
		Region:       *storageRegion,
		AccessKey:    *storageAK,
		SecretKey:    *storageSK,
		SessionToken: *storageToken,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "storage config: %v\n", err)
		os.Exit(1)
	}
	var secondaryStore blob.Store
	var hybridOpts *blob.HybridOptions
	if *hybridProv != "" {
		secondaryStore, err = buildBlobStore(*hybridProv, storageOptions{
			Endpoint:     *hybridEP,
			Bucket:       *hybridBucket,
			Region:       *hybridRegion,
			AccessKey:    *hybridAK,
			SecretKey:    *hybridSK,
			SessionToken: *hybridToken,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "hybrid storage config: %v\n", err)
			os.Exit(1)
		}
		if secondaryStore != nil {
			hybridOpts = &blob.HybridOptions{
				MirrorSecondary: *hybridMirror,
				CacheOnRead:     *hybridCache,
			}
		}
	}
	localBackend, err := localfs.New(ctx, localfs.Config{
		Name:           *storageProv,
		BlobRoot:       *blobRoot,
		BlobStore:      blobStore,
		SecondaryStore: secondaryStore,
		HybridOptions:  hybridOpts,
		MetadataPath:   *metaPath,
		ChunkSize:      int64(*chunkMB) << 20,
		Encrypt:        *encrypt,
		Key:            key,
		CacheEntries:   2048,
		CacheTTL:       time.Minute,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init backend: %v\n", err)
		os.Exit(1)
	}
	var backend fs.Fs = localBackend
	if closer, ok := backend.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	userIdentity := currentUser()
	ctx = fs.NewContextWithUser(ctx, userIdentity)

	switch args[0] {
	case "ls":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "ls requires a path")
			os.Exit(1)
		}
		if err := doList(ctx, backend, args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "ls: %v\n", err)
			os.Exit(1)
		}
	case "put":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "put requires a destination path")
			os.Exit(1)
		}
		if err := doPut(ctx, backend, args[1], os.Stdin); err != nil {
			fmt.Fprintf(os.Stderr, "put: %v\n", err)
			os.Exit(1)
		}
	case "cat":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "cat requires a path")
			os.Exit(1)
		}
		if err := doCat(ctx, backend, args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "cat: %v\n", err)
			os.Exit(1)
		}
	case "cp":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "cp requires source and destination paths")
			os.Exit(1)
		}
		if err := doCopy(ctx, backend, args[1], args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "cp: %v\n", err)
			os.Exit(1)
		}
	case "serve-http":
		if err := runServeHTTP(ctx, backend, args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve-http: %v\n", err)
			os.Exit(1)
		}
	case "serve-s3":
		if err := runServeS3(ctx, backend, args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve-s3: %v\n", err)
			os.Exit(1)
		}
	case "mount-fuse":
		if err := runMountFuse(ctx, backend, args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "mount-fuse: %v\n", err)
			os.Exit(1)
		}
	case "serve-nfs":
		if err := runServeNFS(ctx, backend, args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve-nfs: %v\n", err)
			os.Exit(1)
		}
	case "gc":
		if err := doGC(ctx, backend); err != nil {
			fmt.Fprintf(os.Stderr, "gc: %v\n", err)
			os.Exit(1)
		}
	case "chmod":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "chmod requires <mode> <path>")
			os.Exit(1)
		}
		if err := doChmod(ctx, backend, args[2], args[1], userIdentity); err != nil {
			fmt.Fprintf(os.Stderr, "chmod: %v\n", err)
			os.Exit(1)
		}
	case "chown":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "chown requires <uid[:gid]> <path>")
			os.Exit(1)
		}
		if err := doChown(ctx, backend, args[2], args[1], userIdentity); err != nil {
			fmt.Fprintf(os.Stderr, "chown: %v\n", err)
			os.Exit(1)
		}
	case "rename":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "rename requires <old> <new>")
			os.Exit(1)
		}
		if err := doRename(ctx, backend, args[1], args[2], userIdentity); err != nil {
			fmt.Fprintf(os.Stderr, "rename: %v\n", err)
			os.Exit(1)
		}
	case "mkfifo":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "mkfifo requires <path> [mode]")
			os.Exit(1)
		}
		mode := os.FileMode(0o666)
		if len(args) >= 3 {
			parsed, err := strconv.ParseUint(args[2], 8, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid mode %s: %v\n", args[2], err)
				os.Exit(1)
			}
			mode = os.FileMode(parsed)
		}
		if err := doMkfifo(ctx, backend, args[1], mode, userIdentity); err != nil {
			fmt.Fprintf(os.Stderr, "mkfifo: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s\n", args[0])
		flag.Usage()
		os.Exit(2)
	}
}

func doList(ctx context.Context, backend fs.Fs, p string) error {
	ch, err := backend.List(ctx, p, fs.ListOptions{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TYPE\tSIZE\tPATH")
	for entry := range ch {
		var (
			typ  = "?"
			size int64
			path = pathForEntry(p, entry.Name)
		)
		switch {
		case entry.Object != nil:
			typ = "file"
			size = entry.Object.Size()
		case entry.Dir != nil:
			typ = "dir"
		case entry.Link != nil:
			typ = "link"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\n", typ, size, path)
	}
	return tw.Flush()
}

func doPut(ctx context.Context, backend fs.Fs, dst string, r io.Reader) error {
	obj, err := backend.Create(ctx, dst, fs.CreateOptions{Overwrite: true, Mode: 0o644})
	if err != nil {
		return err
	}
	if _, err := obj.Write(ctx, r, fs.IOOptions{}); err != nil {
		return err
	}
	return nil
}

func doCat(ctx context.Context, backend fs.Fs, src string) error {
	obj, err := backend.Stat(ctx, src)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	writer := &writerAt{Writer: buf}
	if _, err := obj.Read(ctx, writer, fs.IOOptions{}); err != nil {
		return err
	}
	_, err = io.Copy(os.Stdout, buf)
	return err
}

func doCopy(ctx context.Context, backend fs.Fs, src, dst string) error {
	_, err := backend.Copy(ctx, src, dst, fs.CopyOptions{})
	return err
}

func doGC(ctx context.Context, backend fs.Fs) error {
	runner, ok := backend.(interface {
		RunGC(context.Context) (int, error)
	})
	if !ok {
		return errors.New("backend does not expose GC controls")
	}
	count, err := runner.RunGC(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "gc removed %d shards\n", count)
	return nil
}

func doChmod(ctx context.Context, backend fs.Fs, path string, modeStr string, user fs.User) error {
	modeVal, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		return fmt.Errorf("parse mode: %w", err)
	}
	posix, err := requirePosix(backend)
	if err != nil {
		return err
	}
	mode := os.FileMode(modeVal)
	return posix.SetAttr(ctx, path, fs.AttrChanges{Mode: &mode}, user)
}

func doChown(ctx context.Context, backend fs.Fs, path, spec string, user fs.User) error {
	posix, err := requirePosix(backend)
	if err != nil {
		return err
	}
	parts := strings.Split(spec, ":")
	if len(parts) == 0 {
		return fmt.Errorf("invalid uid:gid spec")
	}
	uidVal, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return fmt.Errorf("parse uid: %w", err)
	}
	uid := uint32(uidVal)
	attr := fs.AttrChanges{UID: &uid}
	if len(parts) > 1 && parts[1] != "" {
		gidVal, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("parse gid: %w", err)
		}
		gid := uint32(gidVal)
		attr.GID = &gid
	}
	return posix.SetAttr(ctx, path, attr, user)
}

func doRename(ctx context.Context, backend fs.Fs, oldPath, newPath string, user fs.User) error {
	posix, err := requirePosix(backend)
	if err != nil {
		return err
	}
	return posix.Rename(ctx, oldPath, newPath, fs.RenameOptions{}, user)
}

func doMkfifo(ctx context.Context, backend fs.Fs, path string, mode os.FileMode, user fs.User) error {
	posix, err := requirePosix(backend)
	if err != nil {
		return err
	}
	return posix.Mkfifo(ctx, path, mode, user)
}

type writerAt struct {
	io.Writer
	pos int64
}

func (w *writerAt) WriteAt(p []byte, off int64) (int, error) {
	if off != w.pos {
		return 0, fmt.Errorf("writerAt offset %d not supported (expected %d)", off, w.pos)
	}
	n, err := w.Writer.Write(p)
	w.pos += int64(n)
	return n, err
}

func pathForEntry(base, name string) string {
	base = strings.TrimSuffix(base, "/")
	if base == "" {
		base = "/"
	}
	if name == "" {
		return base
	}
	if base == "/" {
		return "/" + name
	}
	return base + "/" + name
}

type storageOptions struct {
	Endpoint     string
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func buildBlobStore(provider string, opts storageOptions) (blob.Store, error) {
	switch strings.ToLower(provider) {
	case "", "local":
		return nil, nil
	case "s3":
		if opts.Bucket == "" || opts.Region == "" || opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, fmt.Errorf("s3 provider requires bucket, region, access key, and secret key")
		}
		if opts.Endpoint == "" {
			opts.Endpoint = "https://s3.amazonaws.com"
		}
		cfg := blob.S3Config{
			RemoteConfig: blob.RemoteConfig{
				Endpoint: opts.Endpoint,
				Bucket:   opts.Bucket,
			},
			Region:       opts.Region,
			AccessKey:    opts.AccessKey,
			SecretKey:    opts.SecretKey,
			SessionToken: opts.SessionToken,
		}
		return blob.NewS3Store(cfg)
	case "oss":
		if opts.Endpoint == "" || opts.Bucket == "" || opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, fmt.Errorf("oss provider requires endpoint, bucket, access key, and secret key")
		}
		cfg := blob.OSSConfig{
			RemoteConfig: blob.RemoteConfig{
				Endpoint: opts.Endpoint,
				Bucket:   opts.Bucket,
			},
			AccessKey: opts.AccessKey,
			SecretKey: opts.SecretKey,
		}
		return blob.NewOSSStore(cfg)
	case "cos":
		if opts.Endpoint == "" || opts.Bucket == "" || opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, fmt.Errorf("cos provider requires endpoint, bucket, access key, and secret key")
		}
		cfg := blob.COSConfig{
			RemoteConfig: blob.RemoteConfig{
				Endpoint: opts.Endpoint,
				Bucket:   opts.Bucket,
			},
			AccessKey: opts.AccessKey,
			SecretKey: opts.SecretKey,
		}
		return blob.NewCOSStore(cfg)
	default:
		return nil, fmt.Errorf("unknown storage provider %s", provider)
	}
}

func requirePosix(backend fs.Fs) (fs.PosixFs, error) {
	posix, ok := backend.(fs.PosixFs)
	if !ok {
		return nil, errors.New("backend does not implement POSIX operations")
	}
	return posix, nil
}

func currentUser() fs.User {
	u, err := user.Current()
	if err != nil {
		return fs.User{}
	}
	uid, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid, _ := strconv.ParseUint(u.Gid, 10, 32)
	groupIDs, _ := u.GroupIds()
	var supplementary []uint32
	for _, g := range groupIDs {
		if parsed, err := strconv.ParseUint(g, 10, 32); err == nil {
			supplementary = append(supplementary, uint32(parsed))
		}
	}
	return fs.User{
		UID:           uint32(uid),
		GID:           uint32(gid),
		Supplementary: supplementary,
	}
}

func runServeHTTP(ctx context.Context, filesystem fs.Fs, argv []string) error {
	flags := flag.NewFlagSet("serve-http", flag.ContinueOnError)
	addr := flags.String("addr", ":8080", "listen address")
	apiKey := flags.String("api-key", "", "require API key (X-API-Key or Bearer token)")
	pageSize := flags.Int("page-size", 100, "default page size for directory listings")
	pageMax := flags.Int("page-max", 1000, "maximum page size for directory listings")
	rateLimit := flags.Int("rate-limit", 0, "requests allowed per rate window (0 disables)")
	rateWindow := flags.Duration("rate-window", time.Second, "rate limit window")
	if err := flags.Parse(argv); err != nil {
		return err
	}
	httpOpts := httpapi.Options{
		APIKey:          *apiKey,
		DefaultPageSize: *pageSize,
		MaxPageSize:     *pageMax,
	}
	if *rateLimit > 0 {
		httpOpts.RateLimit = middleware.RateLimitOptions{Requests: *rateLimit, Window: *rateWindow}
	}
	server := &httpapi.Server{FS: filesystem, Opts: httpOpts}
	fmt.Fprintf(os.Stderr, "Serving HTTP API on %s\n", *addr)
	if err := server.Start(ctx, *addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runServeS3(ctx context.Context, filesystem fs.Fs, argv []string) error {
	flags := flag.NewFlagSet("serve-s3", flag.ContinueOnError)
	addr := flags.String("addr", ":9000", "listen address")
	bucket := flags.String("bucket", "xgfs", "bucket name exposed via gateway")
	apiKey := flags.String("api-key", "", "require API key (X-API-Key or Bearer token)")
	rateLimit := flags.Int("rate-limit", 0, "requests allowed per rate window (0 disables)")
	rateWindow := flags.Duration("rate-window", time.Second, "rate limit window")
	maxKeys := flags.Int("max-keys", 1000, "maximum keys returned per list response")
	if err := flags.Parse(argv); err != nil {
		return err
	}
	s3Opts := s3gw.Options{
		Bucket:  *bucket,
		APIKey:  *apiKey,
		MaxKeys: *maxKeys,
	}
	if *rateLimit > 0 {
		s3Opts.RateLimit = middleware.RateLimitOptions{Requests: *rateLimit, Window: *rateWindow}
	}
	server := &s3gw.Server{FS: filesystem, Opt: s3Opts}
	fmt.Fprintf(os.Stderr, "Serving S3 gateway on %s (bucket %s)\n", *addr, *bucket)
	if err := server.Start(ctx, *addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runMountFuse(ctx context.Context, filesystem fs.Fs, argv []string) error {
	flags := flag.NewFlagSet("mount-fuse", flag.ContinueOnError)
	mountpoint := flags.String("mountpoint", "", "directory to mount the filesystem")
	if err := flags.Parse(argv); err != nil {
		return err
	}
	if *mountpoint == "" {
		return fmt.Errorf("--mountpoint is required")
	}
	fmt.Fprintf(os.Stderr, "Mounting via FUSE at %s\n", *mountpoint)
	return fuse.Mount(ctx, filesystem, *mountpoint)
}

func runServeNFS(ctx context.Context, filesystem fs.Fs, argv []string) error {
	flags := flag.NewFlagSet("serve-nfs", flag.ContinueOnError)
	addr := flags.String("addr", ":2049", "listen address")
	exportPath := flags.String("export", "/", "filesystem path to export")
	handleCache := flags.Int("handle-cache", 1024, "number of cached NFS file handles")
	if err := flags.Parse(argv); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Serving NFS on %s (export %s)\n", *addr, *exportPath)
	return nfs.ServeWithOptions(ctx, filesystem, *addr, nfs.Options{
		Export:      *exportPath,
		HandleCache: *handleCache,
	})
}

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/jacktea/xgfs/pkg/blob"
	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/server/fuse"
	"github.com/jacktea/xgfs/pkg/server/httpapi"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/server/nfs"
	"github.com/jacktea/xgfs/pkg/server/s3gw"
	"github.com/jacktea/xgfs/pkg/vfs"
)

type app struct {
	ctx        context.Context
	backend    fs.Fs
	vfsBackend *vfs.FS
	rawBackend fs.Fs
	cleanup    func()
	user       vfs.User
}

func (a *app) ensureBackend() error {
	if a.backend != nil {
		return nil
	}
	ctx := context.Background()
	user := currentUser()
	ctx = vfs.NewContextWithUser(ctx, user)

	chunkMB := viper.GetInt("chunk")
	if chunkMB <= 0 {
		chunkMB = 4
	}
	var key []byte
	if viper.GetBool("encrypt") {
		k := viper.GetString("key")
		if k == "" {
			return errors.New("encryption enabled but key missing")
		}
		decoded, err := hex.DecodeString(k)
		if err != nil || len(decoded) != 32 {
			return errors.New("encryption key must be 32 bytes of hex")
		}
		key = decoded
	}

	blobStore, err := buildBlobStore(viper.GetString("storage_provider"), storageOptions{
		Endpoint:     viper.GetString("storage_endpoint"),
		Bucket:       viper.GetString("storage_bucket"),
		Region:       viper.GetString("storage_region"),
		AccessKey:    viper.GetString("storage_access_key"),
		SecretKey:    viper.GetString("storage_secret_key"),
		SessionToken: viper.GetString("storage_session_token"),
	})
	if err != nil {
		return fmt.Errorf("storage config: %w", err)
	}

	var secondary blob.Store
	var hybridOpts *blob.HybridOptions
	if prov := viper.GetString("hybrid_provider"); prov != "" {
		secondary, err = buildBlobStore(prov, storageOptions{
			Endpoint:     viper.GetString("hybrid_endpoint"),
			Bucket:       viper.GetString("hybrid_bucket"),
			Region:       viper.GetString("hybrid_region"),
			AccessKey:    viper.GetString("hybrid_access_key"),
			SecretKey:    viper.GetString("hybrid_secret_key"),
			SessionToken: viper.GetString("hybrid_session_token"),
		})
		if err != nil {
			return fmt.Errorf("hybrid storage config: %w", err)
		}
		if secondary != nil {
			hybridOpts = &blob.HybridOptions{
				MirrorSecondary: viper.GetBool("hybrid_mirror"),
				CacheOnRead:     viper.GetBool("hybrid_cache_read"),
			}
		}
	}

	localBackend, err := localfs.New(ctx, localfs.Config{
		Name:           viper.GetString("storage_provider"),
		BlobRoot:       viper.GetString("root"),
		ChunkSize:      int64(chunkMB) << 20,
		Encrypt:        viper.GetBool("encrypt"),
		Key:            key,
		BlobStore:      blobStore,
		SecondaryStore: secondary,
		HybridOptions:  hybridOpts,
		MetadataPath:   viper.GetString("meta"),
		CacheEntries:   2048,
		CacheTTL:       time.Minute,
	})
	if err != nil {
		return fmt.Errorf("init backend: %w", err)
	}

	metaCacheSize := viper.GetInt("meta_cache_size")
	metaCacheTTL := viper.GetDuration("meta_cache_ttl")
	if metaCacheTTL <= 0 {
		metaCacheTTL = 5 * time.Second
	}

	vBackend := vfs.New(localBackend, vfs.Options{
		MetadataCacheSize: metaCacheSize,
		MetadataCacheTTL:  metaCacheTTL,
	})

	if closer, ok := interface{}(localBackend).(interface{ Close() error }); ok {
		a.cleanup = func() { _ = closer.Close() }
	}
	a.ctx = ctx
	a.backend = vBackend
	a.vfsBackend = vBackend
	a.rawBackend = localBackend
	a.user = user
	return nil
}

func (a *app) close() {
	if a.cleanup != nil {
		a.cleanup()
	}
}

var (
	cfgFile     string
	application = &app{}
	rootCmd     = &cobra.Command{
		Use:           "xgfs",
		Short:         "xgfs virtual filesystem CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return application.ensureBackend()
		},
	}
)

func init() {
	cobra.OnInitialize(initConfig)
	initRootFlags()
	initCommands()
}

func main() {
	defer application.close()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("xgfs")
		viper.AddConfigPath(".")
		if home, err := os.UserHomeDir(); err == nil {
			viper.AddConfigPath(filepath.Join(home, ".config", "xgfs"))
		}
	}
	viper.SetEnvPrefix("XGFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		var nf viper.ConfigFileNotFoundError
		if !errors.As(err, &nf) {
			fmt.Fprintf(os.Stderr, "read config: %v\n", err)
		}
	}
}

func bindConfig(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(err)
	}
}

func initRootFlags() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (TOML or YAML)")

	rootCmd.PersistentFlags().String("root", ".xgfs/blobs", "blob storage root (local provider)")
	rootCmd.PersistentFlags().Int("chunk", 4, "shard size in MiB")
	rootCmd.PersistentFlags().Bool("encrypt", false, "enable shard encryption")
	rootCmd.PersistentFlags().String("key", "", "hex-encoded 32-byte key when encryption enabled")
	rootCmd.PersistentFlags().String("meta", "", "path to metadata file store")

	rootCmd.PersistentFlags().String("storage-provider", "local", "storage provider: local|s3|oss|cos")
	rootCmd.PersistentFlags().String("storage-endpoint", "", "remote storage endpoint")
	rootCmd.PersistentFlags().String("storage-bucket", "", "remote storage bucket name")
	rootCmd.PersistentFlags().String("storage-region", "", "region (S3 only)")
	rootCmd.PersistentFlags().String("storage-access-key", "", "remote storage access key")
	rootCmd.PersistentFlags().String("storage-secret-key", "", "remote storage secret key")
	rootCmd.PersistentFlags().String("storage-session-token", "", "remote storage session token (S3)")

	rootCmd.PersistentFlags().String("hybrid-provider", "", "secondary storage provider for hybrid tier")
	rootCmd.PersistentFlags().String("hybrid-endpoint", "", "secondary storage endpoint")
	rootCmd.PersistentFlags().String("hybrid-bucket", "", "secondary storage bucket")
	rootCmd.PersistentFlags().String("hybrid-region", "", "secondary storage region (S3 only)")
	rootCmd.PersistentFlags().String("hybrid-access-key", "", "secondary storage access key")
	rootCmd.PersistentFlags().String("hybrid-secret-key", "", "secondary storage secret key")
	rootCmd.PersistentFlags().String("hybrid-session-token", "", "secondary storage session token (S3)")
	rootCmd.PersistentFlags().Bool("hybrid-mirror", true, "mirror writes to the secondary store")
	rootCmd.PersistentFlags().Bool("hybrid-cache-read", true, "cache secondary reads into the primary store")

	rootCmd.PersistentFlags().Int("meta-cache-size", 0, "entries cached by the vfs metadata layer (0 disables)")
	rootCmd.PersistentFlags().Duration("meta-cache-ttl", 5*time.Second, "time to keep vfs metadata cache entries")

	bindConfig("root", rootCmd.PersistentFlags().Lookup("root"))
	bindConfig("chunk", rootCmd.PersistentFlags().Lookup("chunk"))
	bindConfig("encrypt", rootCmd.PersistentFlags().Lookup("encrypt"))
	bindConfig("key", rootCmd.PersistentFlags().Lookup("key"))
	bindConfig("meta", rootCmd.PersistentFlags().Lookup("meta"))

	bindConfig("storage_provider", rootCmd.PersistentFlags().Lookup("storage-provider"))
	bindConfig("storage_endpoint", rootCmd.PersistentFlags().Lookup("storage-endpoint"))
	bindConfig("storage_bucket", rootCmd.PersistentFlags().Lookup("storage-bucket"))
	bindConfig("storage_region", rootCmd.PersistentFlags().Lookup("storage-region"))
	bindConfig("storage_access_key", rootCmd.PersistentFlags().Lookup("storage-access-key"))
	bindConfig("storage_secret_key", rootCmd.PersistentFlags().Lookup("storage-secret-key"))
	bindConfig("storage_session_token", rootCmd.PersistentFlags().Lookup("storage-session-token"))

	bindConfig("hybrid_provider", rootCmd.PersistentFlags().Lookup("hybrid-provider"))
	bindConfig("hybrid_endpoint", rootCmd.PersistentFlags().Lookup("hybrid-endpoint"))
	bindConfig("hybrid_bucket", rootCmd.PersistentFlags().Lookup("hybrid-bucket"))
	bindConfig("hybrid_region", rootCmd.PersistentFlags().Lookup("hybrid-region"))
	bindConfig("hybrid_access_key", rootCmd.PersistentFlags().Lookup("hybrid-access-key"))
	bindConfig("hybrid_secret_key", rootCmd.PersistentFlags().Lookup("hybrid-secret-key"))
	bindConfig("hybrid_session_token", rootCmd.PersistentFlags().Lookup("hybrid-session-token"))
	bindConfig("hybrid_mirror", rootCmd.PersistentFlags().Lookup("hybrid-mirror"))
	bindConfig("hybrid_cache_read", rootCmd.PersistentFlags().Lookup("hybrid-cache-read"))

	bindConfig("meta_cache_size", rootCmd.PersistentFlags().Lookup("meta-cache-size"))
	bindConfig("meta_cache_ttl", rootCmd.PersistentFlags().Lookup("meta-cache-ttl"))
}

func initCommands() {
	rootCmd.AddCommand(
		newLsCmd(),
		newPutCmd(),
		newCatCmd(),
		newCpCmd(),
		newServeHTTPCmd(),
		newServeS3Cmd(),
		newMountFuseCmd(),
		newServeNFSCmd(),
		newGCCmd(),
		newChmodCmd(),
		newChownCmd(),
		newRenameCmd(),
		newMkfifoCmd(),
	)
}

func appContext() (context.Context, fs.Fs) {
	return application.ctx, application.backend
}

func posixBackend() vfs.PosixFs {
	return application.vfsBackend
}

func serveFilesystem() interface {
	fs.Fs
	vfs.PosixFs
} {
	return application.vfsBackend
}

func newLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls <path>",
		Short: "List directory contents",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, backend := appContext()
			return doList(ctx, backend, args[0])
		},
	}
}

func newPutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "put <destination>",
		Short: "Write stdin to the destination path",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, backend := appContext()
			return doPut(ctx, backend, args[0], os.Stdin)
		},
	}
}

func newCatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cat <path>",
		Short: "Print the file contents",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, backend := appContext()
			return doCat(ctx, backend, args[0])
		},
	}
}

func newCpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cp <source> <destination>",
		Short: "Copy a file",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, backend := appContext()
			return doCopy(ctx, backend, args[0], args[1])
		},
	}
}

func newGCCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gc",
		Short: "Run garbage collection",
		RunE: func(cmd *cobra.Command, args []string) error {
			runner, ok := interface{}(application.rawBackend).(gcRunner)
			if !ok {
				return errors.New("backend does not support garbage collection")
			}
			return doGC(application.ctx, runner)
		},
	}
}

func newChmodCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "chmod <mode> <path>",
		Short: "Change file mode bits",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := application.ctx
			return doChmod(ctx, posixBackend(), args[1], args[0], application.user)
		},
	}
}

func newChownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "chown <uid[:gid]> <path>",
		Short: "Change file ownership",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return doChown(application.ctx, posixBackend(), args[1], args[0], application.user)
		},
	}
}

func newRenameCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rename <old> <new>",
		Short: "Rename a file or directory",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return doRename(application.ctx, posixBackend(), args[0], args[1], application.user)
		},
	}
}

func newMkfifoCmd() *cobra.Command {
	var modeStr string
	cmd := &cobra.Command{
		Use:   "mkfifo <path>",
		Short: "Create a FIFO special file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode := os.FileMode(0o666)
			if modeStr != "" {
				value, err := strconv.ParseUint(modeStr, 8, 32)
				if err != nil {
					return fmt.Errorf("invalid mode: %w", err)
				}
				mode = os.FileMode(value)
			}
			return doMkfifo(application.ctx, posixBackend(), args[0], mode, application.user)
		},
	}
	cmd.Flags().StringVar(&modeStr, "mode", "", "octal mode for the FIFO (default 0666)")
	return cmd
}

func newServeHTTPCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve-http",
		Short: "Expose the filesystem over HTTP",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := httpServeOptions{
				Addr:       viper.GetString("serve_http.addr"),
				APIKey:     viper.GetString("serve_http.api_key"),
				PageSize:   viper.GetInt("serve_http.page_size"),
				PageMax:    viper.GetInt("serve_http.page_max"),
				RateLimit:  viper.GetInt("serve_http.rate_limit"),
				RateWindow: viper.GetDuration("serve_http.rate_window"),
			}
			return runServeHTTP(application.ctx, serveFilesystem(), opts)
		},
	}
	cmd.Flags().String("addr", ":8080", "listen address")
	cmd.Flags().String("api-key", "", "require API key (X-API-Key or Bearer token)")
	cmd.Flags().Int("page-size", 100, "default page size for directory listings")
	cmd.Flags().Int("page-max", 1000, "maximum page size for directory listings")
	cmd.Flags().Int("rate-limit", 0, "requests allowed per rate window (0 disables)")
	cmd.Flags().Duration("rate-window", time.Second, "rate limit window")
	bindConfig("serve_http.addr", cmd.Flags().Lookup("addr"))
	bindConfig("serve_http.api_key", cmd.Flags().Lookup("api-key"))
	bindConfig("serve_http.page_size", cmd.Flags().Lookup("page-size"))
	bindConfig("serve_http.page_max", cmd.Flags().Lookup("page-max"))
	bindConfig("serve_http.rate_limit", cmd.Flags().Lookup("rate-limit"))
	bindConfig("serve_http.rate_window", cmd.Flags().Lookup("rate-window"))
	return cmd
}

func newServeS3Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve-s3",
		Short: "Expose an S3-compatible gateway",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := s3ServeOptions{
				Addr:       viper.GetString("serve_s3.addr"),
				Bucket:     viper.GetString("serve_s3.bucket"),
				APIKey:     viper.GetString("serve_s3.api_key"),
				RateLimit:  viper.GetInt("serve_s3.rate_limit"),
				RateWindow: viper.GetDuration("serve_s3.rate_window"),
				MaxKeys:    viper.GetInt("serve_s3.max_keys"),
			}
			return runServeS3(application.ctx, serveFilesystem(), opts)
		},
	}
	cmd.Flags().String("addr", ":9000", "listen address")
	cmd.Flags().String("bucket", "xgfs", "bucket name exposed via gateway")
	cmd.Flags().String("api-key", "", "require API key (X-API-Key header)")
	cmd.Flags().Int("rate-limit", 0, "requests allowed per rate window (0 disables)")
	cmd.Flags().Duration("rate-window", time.Second, "rate limit window")
	cmd.Flags().Int("max-keys", 1000, "maximum keys returned per list response")
	bindConfig("serve_s3.addr", cmd.Flags().Lookup("addr"))
	bindConfig("serve_s3.bucket", cmd.Flags().Lookup("bucket"))
	bindConfig("serve_s3.api_key", cmd.Flags().Lookup("api-key"))
	bindConfig("serve_s3.rate_limit", cmd.Flags().Lookup("rate-limit"))
	bindConfig("serve_s3.rate_window", cmd.Flags().Lookup("rate-window"))
	bindConfig("serve_s3.max_keys", cmd.Flags().Lookup("max-keys"))
	return cmd
}

func newServeNFSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve-nfs",
		Short: "Expose the filesystem over NFS",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := nfsServeOptions{
				Addr:        viper.GetString("serve_nfs.addr"),
				Export:      viper.GetString("serve_nfs.export"),
				HandleCache: viper.GetInt("serve_nfs.handle_cache"),
			}
			return runServeNFS(application.ctx, serveFilesystem(), opts)
		},
	}
	cmd.Flags().String("addr", ":2049", "listen address")
	cmd.Flags().String("export", "/", "filesystem path to export")
	cmd.Flags().Int("handle-cache", 1024, "number of cached NFS file handles")
	bindConfig("serve_nfs.addr", cmd.Flags().Lookup("addr"))
	bindConfig("serve_nfs.export", cmd.Flags().Lookup("export"))
	bindConfig("serve_nfs.handle_cache", cmd.Flags().Lookup("handle-cache"))
	return cmd
}

func newMountFuseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount-fuse",
		Short: "Mount the filesystem via FUSE",
		RunE: func(cmd *cobra.Command, args []string) error {
			mountpoint := viper.GetString("mount_fuse.mountpoint")
			if mountpoint == "" {
				return errors.New("--mountpoint is required")
			}
			return runMountFuse(application.ctx, application.backend, mountpoint)
		},
	}
	cmd.Flags().String("mountpoint", "", "directory to mount the filesystem")
	bindConfig("mount_fuse.mountpoint", cmd.Flags().Lookup("mountpoint"))
	return cmd
}

type httpServeOptions struct {
	Addr       string
	APIKey     string
	PageSize   int
	PageMax    int
	RateLimit  int
	RateWindow time.Duration
}

type s3ServeOptions struct {
	Addr       string
	Bucket     string
	APIKey     string
	RateLimit  int
	RateWindow time.Duration
	MaxKeys    int
}

type nfsServeOptions struct {
	Addr        string
	Export      string
	HandleCache int
}

func buildBlobStore(provider string, opts storageOptions) (blob.Store, error) {
	switch strings.ToLower(provider) {
	case "", "local":
		return nil, nil
	case "s3":
		if opts.Endpoint == "" || opts.Bucket == "" || opts.AccessKey == "" || opts.SecretKey == "" || opts.Region == "" {
			return nil, errors.New("s3 config requires endpoint, bucket, region, access key, and secret key")
		}
		return blob.NewS3Store(blob.S3Config{
			RemoteConfig: blob.RemoteConfig{
				Endpoint:     opts.Endpoint,
				Bucket:       opts.Bucket,
				CacheEntries: 1024,
				CacheTTL:     time.Minute,
			},
			Region:       opts.Region,
			AccessKey:    opts.AccessKey,
			SecretKey:    opts.SecretKey,
			SessionToken: opts.SessionToken,
		})
	case "oss":
		if opts.Endpoint == "" || opts.Bucket == "" || opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, errors.New("oss config requires endpoint, bucket, access key, and secret key")
		}
		return blob.NewOSSStore(blob.OSSConfig{
			RemoteConfig: blob.RemoteConfig{
				Endpoint:     opts.Endpoint,
				Bucket:       opts.Bucket,
				CacheEntries: 1024,
				CacheTTL:     time.Minute,
			},
			AccessKey: opts.AccessKey,
			SecretKey: opts.SecretKey,
		})
	case "cos":
		if opts.Endpoint == "" || opts.Bucket == "" || opts.AccessKey == "" || opts.SecretKey == "" {
			return nil, errors.New("cos config requires endpoint, bucket, access key, and secret key")
		}
		return blob.NewCOSStore(blob.COSConfig{
			RemoteConfig: blob.RemoteConfig{
				Endpoint:     opts.Endpoint,
				Bucket:       opts.Bucket,
				CacheEntries: 1024,
				CacheTTL:     time.Minute,
			},
			AccessKey: opts.AccessKey,
			SecretKey: opts.SecretKey,
		})
	default:
		return nil, fmt.Errorf("unknown storage provider %q", provider)
	}
}

type storageOptions struct {
	Endpoint     string
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func runServeHTTP(ctx context.Context, filesystem interface {
	fs.Fs
	vfs.PosixFs
}, opt httpServeOptions) error {
	httpOpts := httpapi.Options{
		APIKey:          opt.APIKey,
		DefaultPageSize: opt.PageSize,
		MaxPageSize:     opt.PageMax,
	}
	if opt.RateLimit > 0 {
		httpOpts.RateLimit = middleware.RateLimitOptions{
			Requests: opt.RateLimit,
			Window:   opt.RateWindow,
		}
	}
	server := &httpapi.Server{FS: filesystem, Opts: httpOpts}
	fmt.Fprintf(os.Stderr, "Serving HTTP API on %s\n", opt.Addr)
	if err := server.Start(ctx, opt.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runServeS3(ctx context.Context, filesystem interface {
	fs.Fs
	vfs.PosixFs
}, opt s3ServeOptions) error {
	if opt.Bucket == "" {
		return errors.New("serve-s3: --bucket is required")
	}
	s3Opts := s3gw.Options{
		Bucket:  opt.Bucket,
		APIKey:  opt.APIKey,
		MaxKeys: opt.MaxKeys,
	}
	if opt.RateLimit > 0 {
		s3Opts.RateLimit = middleware.RateLimitOptions{Requests: opt.RateLimit, Window: opt.RateWindow}
	}
	server := &s3gw.Server{FS: filesystem, Opt: s3Opts}
	fmt.Fprintf(os.Stderr, "Serving S3 gateway on %s (bucket %s)\n", opt.Addr, opt.Bucket)
	if err := server.Start(ctx, opt.Addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runMountFuse(ctx context.Context, filesystem fs.Fs, mountpoint string) error {
	fmt.Fprintf(os.Stderr, "Mounting via FUSE at %s\n", mountpoint)
	return fuse.Mount(ctx, filesystem, mountpoint)
}

func runServeNFS(ctx context.Context, filesystem interface {
	fs.Fs
	vfs.PosixFs
}, opt nfsServeOptions) error {
	if opt.HandleCache <= 0 {
		opt.HandleCache = 1024
	}
	fmt.Fprintf(os.Stderr, "Serving NFS on %s (export %s)\n", opt.Addr, opt.Export)
	return nfs.ServeWithOptions(ctx, filesystem, opt.Addr, nfs.Options{
		Export:      opt.Export,
		HandleCache: opt.HandleCache,
	})
}

func doList(ctx context.Context, backend fs.Fs, p string) error {
	ch, err := backend.List(ctx, p, fs.ListOptions{})
	if err != nil {
		return err
	}
	for entry := range ch {
		switch {
		case entry.Dir != nil:
			fmt.Printf("%s/\n", entry.Name)
		case entry.Object != nil:
			fmt.Printf("%s\t%d\n", entry.Name, entry.Object.Size())
		case entry.Link != nil:
			fmt.Printf("%s -> %s\n", entry.Name, entry.Link.Target)
		default:
			fmt.Println(entry.Name)
		}
	}
	return nil
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
	writer := &writerAt{Writer: os.Stdout}
	if _, err := obj.Read(ctx, writer, fs.IOOptions{}); err != nil {
		return err
	}
	return nil
}

func doCopy(ctx context.Context, backend fs.Fs, src, dst string) error {
	_, err := backend.Copy(ctx, src, dst, fs.CopyOptions{})
	return err
}

type gcRunner interface {
	RunGC(context.Context) (int, error)
}

func doGC(ctx context.Context, runner gcRunner) error {
	count, err := runner.RunGC(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "gc removed %d shards\n", count)
	return nil
}

func doChmod(ctx context.Context, backend vfs.PosixFs, path string, modeStr string, user vfs.User) error {
	value, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid mode %s", modeStr)
	}
	mode := os.FileMode(value)
	return backend.SetAttr(ctx, path, vfs.AttrChanges{Mode: &mode}, user)
}

func doChown(ctx context.Context, backend vfs.PosixFs, path, spec string, user vfs.User) error {
	parts := strings.Split(spec, ":")
	if len(parts) == 0 || parts[0] == "" {
		return fmt.Errorf("invalid uid:gid spec %s", spec)
	}
	uidVal, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return fmt.Errorf("parse uid: %w", err)
	}
	uid := uint32(uidVal)
	attr := vfs.AttrChanges{UID: &uid}
	if len(parts) > 1 && parts[1] != "" {
		gidVal, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("parse gid: %w", err)
		}
		gid := uint32(gidVal)
		attr.GID = &gid
	}
	return backend.SetAttr(ctx, path, attr, user)
}

func doRename(ctx context.Context, backend vfs.PosixFs, oldPath, newPath string, user vfs.User) error {
	return backend.Rename(ctx, oldPath, newPath, vfs.RenameOptions{}, user)
}

func doMkfifo(ctx context.Context, backend vfs.PosixFs, path string, mode os.FileMode, user vfs.User) error {
	return backend.Mkfifo(ctx, path, mode, user)
}

type writerAt struct {
	io.Writer
}

func (w *writerAt) WriteAt(p []byte, off int64) (int, error) {
	return w.Writer.Write(p)
}

func currentUser() vfs.User {
	u, err := user.Current()
	if err != nil {
		return vfs.User{}
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
	return vfs.User{
		UID:           uint32(uid),
		GID:           uint32(gid),
		Supplementary: supplementary,
	}
}

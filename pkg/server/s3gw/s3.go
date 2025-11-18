package s3gw

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/johannesboyne/gofakes3"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/vfs"
)

// Options configure the S3 gateway.
type Options struct {
	Bucket    string
	APIKey    string
	RateLimit middleware.RateLimitOptions
	MaxKeys   int
}

// Filesystem combines fs.Fs semantics with POSIX extensions expected by S3
// metadata/rename endpoints.
type Filesystem interface {
	fs.Fs
	vfs.PosixFs
}

// Server exposes a tiny subset of the S3 API backed by Filesystem.
type Server struct {
	FS  Filesystem
	Opt Options

	handlerOnce sync.Once
	handler     http.Handler
}

// Start listens on addr until ctx is canceled.
func (s *Server) Start(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.httpHandler()}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	return srv.ListenAndServe()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpHandler().ServeHTTP(w, r)
}

func (s *Server) objectKey(p string) string {
	cleaned := path.Clean("/" + strings.TrimPrefix(p, "/"))
	if cleaned == "/" {
		if s.Opt.Bucket != "" {
			return path.Clean("/" + s.Opt.Bucket)
		}
		return cleaned
	}
	if s.Opt.Bucket == "" {
		return cleaned
	}
	trimmed := strings.TrimPrefix(cleaned, "/")
	if trimmed == s.Opt.Bucket || strings.HasPrefix(trimmed, s.Opt.Bucket+"/") {
		return cleaned
	}
	return path.Join("/", s.Opt.Bucket, trimmed)
}

func (s *Server) httpHandler() http.Handler {
	s.handlerOnce.Do(func() {
		backend := NewBackend(s.FS)
		s3 := gofakes3.New(backend).Server()
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if s.handleRename(w, r) {
				return
			}
			s.ensureContentLength(r)
			s.rewriteBucketPath(r)
			s3.ServeHTTP(w, r)
		})
		if chain := s.middlewares(); len(chain) > 0 {
			handler = middleware.Wrap(handler, chain...)
		}
		s.handler = handler
	})
	return s.handler
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	renameTo := r.URL.Query().Get("rename")
	if renameTo == "" {
		return false
	}
	src := s.objectKey(r.URL.Path)
	dst := s.objectKey(renameTo)
	if err := s.FS.Rename(r.Context(), src, dst, vfs.RenameOptions{}, vfs.User{}); err != nil {
		http.Error(w, err.Error(), statusFromError(err))
		return true
	}
	w.WriteHeader(http.StatusOK)
	return true
}

func (s *Server) rewriteBucketPath(r *http.Request) {
	if s.Opt.Bucket == "" {
		return
	}
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	if trimmed == "" {
		return
	}
	if strings.HasPrefix(trimmed, s.Opt.Bucket+"/") || trimmed == s.Opt.Bucket {
		return
	}
	newPath := path.Join("/", s.Opt.Bucket, trimmed)
	r.URL.Path = newPath
	r.URL.RawPath = newPath
}

func (s *Server) ensureContentLength(r *http.Request) {
	if r.Header.Get("Content-Length") != "" || r.ContentLength < 0 {
		return
	}
	r.Header.Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
}

func (s *Server) middlewares() []middleware.HTTPMiddleware {
	var chain []middleware.HTTPMiddleware
	if auth := middleware.APIKeyAuth(s.Opt.APIKey); auth != nil {
		chain = append(chain, auth)
	}
	if limit := middleware.RateLimit(s.Opt.RateLimit); limit != nil {
		chain = append(chain, limit)
	}
	return chain
}

func statusFromError(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, fs.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, fs.ErrAlreadyExist):
		return http.StatusConflict
	case errors.Is(err, fs.ErrNotSupported):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

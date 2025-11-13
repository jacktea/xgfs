package s3gw

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
)

// Options configure the S3 gateway.
type Options struct {
	Bucket    string
	APIKey    string
	RateLimit middleware.RateLimitOptions
	MaxKeys   int
}

// Server exposes a tiny subset of the S3 API backed by fs.Fs.
type Server struct {
	FS  fs.Fs
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

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if strings.HasPrefix(r.URL.RawQuery, "list-type=2") || r.URL.Query().Get("list-type") == "2" {
			s.listObjects(w, r)
			return
		}
		s.getObject(w, r)
	case http.MethodPut:
		s.putObject(w, r)
	case http.MethodDelete:
		s.deleteObject(w, r)
	case http.MethodPost:
		s.postObject(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	limit, token := s.listingBounds(r)
	opts := fs.ListOptions{Limit: limit + 1, StartAfter: token}
	ch, err := s.FS.List(r.Context(), "/"+prefix, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type Contents struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	}
	res := struct {
		XMLName               xml.Name   `xml:"ListBucketResult"`
		Name                  string     `xml:"Name"`
		Prefix                string     `xml:"Prefix"`
		KeyCount              int        `xml:"KeyCount"`
		MaxKeys               int        `xml:"MaxKeys"`
		IsTruncated           bool       `xml:"IsTruncated"`
		ContinuationToken     string     `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string     `xml:"NextContinuationToken,omitempty"`
		Contents              []Contents `xml:"Contents"`
	}{
		Name:              s.Opt.Bucket,
		Prefix:            prefix,
		MaxKeys:           limit,
		ContinuationToken: token,
	}
	for entry := range ch {
		if entry.Object != nil {
			res.Contents = append(res.Contents, Contents{Key: strings.TrimPrefix(path.Join(prefix, entry.Name), "/"), Size: entry.Object.Size()})
			if len(res.Contents) >= limit+1 {
				break
			}
		}
	}
	if len(res.Contents) > limit {
		res.IsTruncated = true
		res.NextContinuationToken = res.Contents[limit-1].Key
		res.Contents = res.Contents[:limit]
	}
	res.KeyCount = len(res.Contents)
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	key := s.objectKey(r.URL.Path)
	obj, err := s.FS.Stat(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size()))
	if meta := obj.Metadata(); meta.Mode != 0 {
		w.Header().Set("X-Amz-Meta-Posix-Mode", fmt.Sprintf("%o", meta.Mode))
		w.Header().Set("X-Amz-Meta-Posix-Uid", fmt.Sprintf("%d", meta.UID))
		w.Header().Set("X-Amz-Meta-Posix-Gid", fmt.Sprintf("%d", meta.GID))
	}
	if r.Method == http.MethodHead {
		return
	}
	writer := &sequentialWriter{w: w}
	if _, err := obj.Read(r.Context(), writer, fs.IOOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	key := s.objectKey(r.URL.Path)
	obj, err := s.FS.Create(r.Context(), key, fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := obj.Write(r.Context(), r.Body, fs.IOOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.applyObjectMetadata(r.Context(), key, r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	key := s.objectKey(r.URL.Path)
	if err := s.FS.Remove(r.Context(), key, fs.RemoveOptions{}); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) objectKey(p string) string {
	cleaned := path.Clean("/" + strings.TrimPrefix(p, "/"))
	if cleaned == "/" {
		return "/"
	}
	return cleaned
}

type sequentialWriter struct {
	w   http.ResponseWriter
	pos int64
}

func (s *sequentialWriter) WriteAt(p []byte, off int64) (int, error) {
	if off != s.pos {
		return 0, fmt.Errorf("non-sequential write")
	}
	n, err := s.w.Write(p)
	s.pos += int64(n)
	return n, err
}

func (s *Server) httpHandler() http.Handler {
	s.handlerOnce.Do(func() {
		var handler http.Handler = http.HandlerFunc(s.dispatch)
		if chain := s.middlewares(); len(chain) > 0 {
			handler = middleware.Wrap(handler, chain...)
		}
		s.handler = handler
	})
	return s.handler
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

func (s *Server) listingBounds(r *http.Request) (limit int, token string) {
	limit = s.maxKeys()
	if raw := r.URL.Query().Get("max-keys"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n < limit {
				limit = n
			}
		}
	}
	token = r.URL.Query().Get("continuation-token")
	if token == "" {
		token = r.URL.Query().Get("start-after")
	}
	return limit, token
}

func (s *Server) maxKeys() int {
	if s.Opt.MaxKeys > 0 {
		return s.Opt.MaxKeys
	}
	return 1000
}

func (s *Server) applyObjectMetadata(ctx context.Context, path string, r *http.Request) error {
	posix, ok := s.FS.(fs.PosixFs)
	if !ok {
		return nil
	}
	modeHeader := r.Header.Get("X-Amz-Meta-Posix-Mode")
	uidHeader := r.Header.Get("X-Amz-Meta-Posix-Uid")
	gidHeader := r.Header.Get("X-Amz-Meta-Posix-Gid")
	var changes fs.AttrChanges
	if modeHeader != "" {
		value, err := strconv.ParseUint(modeHeader, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid posix mode: %w", err)
		}
		mode := os.FileMode(value)
		changes.Mode = &mode
	}
	if uidHeader != "" {
		value, err := strconv.ParseUint(uidHeader, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid uid: %w", err)
		}
		uid := uint32(value)
		changes.UID = &uid
	}
	if gidHeader != "" {
		value, err := strconv.ParseUint(gidHeader, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid gid: %w", err)
		}
		gid := uint32(value)
		changes.GID = &gid
	}
	if changes.Mode == nil && changes.UID == nil && changes.GID == nil {
		return nil
	}
	user := fs.User{}
	return posix.SetAttr(ctx, path, changes, user)
}

func (s *Server) postObject(w http.ResponseWriter, r *http.Request) {
	posix, ok := s.FS.(fs.PosixFs)
	if !ok {
		http.Error(w, "posix operations not supported", http.StatusNotImplemented)
		return
	}
	renameTo := r.URL.Query().Get("rename")
	if renameTo == "" {
		http.Error(w, "missing rename target", http.StatusBadRequest)
		return
	}
	src := s.objectKey(r.URL.Path)
	dst := s.objectKey(renameTo)
	if err := posix.Rename(r.Context(), src, dst, fs.RenameOptions{}, fs.User{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/vfs"
	"github.com/jacktea/xgfs/pkg/xerrors"
)

// Filesystem combines the base fs.Fs surface with POSIX extensions. Callers
// should supply a vfs.FS (or equivalent adapter) so HTTP operations never have
// to guess whether rename/chmod/mkfifo exist.
type Filesystem interface {
	fs.Fs
	vfs.PosixFs
}

// Server exposes Filesystem over a simple HTTP+JSON API.
type Server struct {
	FS   Filesystem
	Log  *log.Logger
	Opts Options
}

// Options configure auth, pagination, and rate limiting.
type Options struct {
	APIKey          string
	RateLimit       middleware.RateLimitOptions
	DefaultPageSize int
	MaxPageSize     int
}

// Start begins listening on addr until ctx is canceled.
func (s *Server) Start(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.router()}
	go func() {
		<-ctx.Done()
		ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctxShutdown)
	}()
	return srv.ListenAndServe()
}

func (s *Server) router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/files/", s.handleFiles)
	mux.HandleFunc("/dirs/", s.handleDirs)
	return s.applyMiddleware(mux)
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p := cleanPath(strings.TrimPrefix(r.URL.Path, "/files"))
	switch r.Method {
	case http.MethodGet:
		s.serveFile(ctx, w, r, p)
	case http.MethodHead:
		s.headFile(ctx, w, r, p)
	case http.MethodPut:
		s.putFile(ctx, w, r, p)
	case http.MethodPost:
		s.postFile(ctx, w, r, p)
	case http.MethodDelete:
		if err := s.FS.Remove(ctx, p, fs.RemoveOptions{}); err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		s.patchFile(ctx, w, r, p)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) serveFile(ctx context.Context, w http.ResponseWriter, r *http.Request, p string) {
	obj, err := s.FS.Stat(ctx, p)
	if err != nil {
		httpError(w, err)
		return
	}
	size := obj.Size()
	w.Header().Set("Accept-Ranges", "bytes")
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		start, end, parseErr := parseRangeHeader(rangeHeader, size)
		if parseErr != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		length := end - start + 1
		buf := make([]byte, length)
		n, err := obj.ReadAt(ctx, buf, start, fs.IOOptions{})
		if err != nil && !errors.Is(err, io.EOF) {
			httpError(w, err)
			return
		}
		if n == 0 {
			httpError(w, fs.ErrNotFound)
			return
		}
		actualEnd := start + int64(n) - 1
		w.Header().Set("Content-Length", fmt.Sprintf("%d", n))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, actualEnd, size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(buf[:n])
		return
	}
	reader := &sequentialWriter{w: w}
	if _, err := obj.Read(ctx, reader, fs.IOOptions{}); err != nil {
		httpError(w, err)
		return
	}
}

func (s *Server) headFile(ctx context.Context, w http.ResponseWriter, _ *http.Request, p string) {
	obj, err := s.FS.Stat(ctx, p)
	if err != nil {
		httpError(w, err)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size()))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) putFile(ctx context.Context, w http.ResponseWriter, r *http.Request, p string) {
	obj, err := s.FS.Create(ctx, p, fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		httpError(w, err)
		return
	}
	if _, err := obj.Write(ctx, r.Body, fs.IOOptions{}); err != nil {
		httpError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleDirs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p := cleanPath(strings.TrimPrefix(r.URL.Path, "/dirs"))
	limit, token := s.listingParams(r)
	opts := fs.ListOptions{Limit: limit + 1, StartAfter: token}
	ch, err := s.FS.List(ctx, p, opts)
	if err != nil {
		httpError(w, err)
		return
	}
	entries := make([]dirEntry, 0, limit)
	for entry := range ch {
		entries = append(entries, toDirEntry(entry))
		if len(entries) >= limit+1 {
			break
		}
	}
	var nextToken string
	if len(entries) > limit {
		nextToken = entries[limit-1].Name
		entries = entries[:limit]
	}
	response := struct {
		Entries       []dirEntry `json:"entries"`
		NextPageToken string     `json:"next_page_token,omitempty"`
	}{
		Entries: entries,
	}
	if nextToken != "" {
		response.NextPageToken = nextToken
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	out := path.Clean("/" + strings.TrimPrefix(p, "/"))
	if out == "" {
		return "/"
	}
	return out
}

func httpError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, fs.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, fs.ErrAlreadyExist):
		status = http.StatusConflict
	case errors.Is(err, fs.ErrNotSupported):
		status = http.StatusNotImplemented
	default:
		switch xerrors.KindOf(err) {
		case xerrors.KindNotFound:
			status = http.StatusNotFound
		case xerrors.KindAlreadyExists:
			status = http.StatusConflict
		case xerrors.KindPermission:
			status = http.StatusForbidden
		case xerrors.KindRange:
			status = http.StatusRequestedRangeNotSatisfiable
		case xerrors.KindInvalid:
			status = http.StatusBadRequest
		case xerrors.KindNotSupported:
			status = http.StatusNotImplemented
		}
	}
	http.Error(w, err.Error(), status)
}

type attrPayload struct {
	Mode     *uint32           `json:"mode"`
	UID      *uint32           `json:"uid"`
	GID      *uint32           `json:"gid"`
	Size     *int64            `json:"size"`
	XAttrs   map[string]string `json:"xattrs"`
	RenameTo string            `json:"rename_to"`
}

type fifoPayload struct {
	Type string  `json:"type"`
	Mode *uint32 `json:"mode"`
}

func (s *Server) patchFile(ctx context.Context, w http.ResponseWriter, r *http.Request, p string) {
	var payload attrPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	user := userFromRequest(r)
	if payload.RenameTo != "" {
		target := cleanPath(payload.RenameTo)
		if err := s.FS.Rename(ctx, p, target, vfs.RenameOptions{}, user); err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	changes := vfs.AttrChanges{}
	if payload.Mode != nil {
		mode := os.FileMode(*payload.Mode)
		changes.Mode = &mode
	}
	if payload.UID != nil {
		uid := *payload.UID
		changes.UID = &uid
	}
	if payload.GID != nil {
		gid := *payload.GID
		changes.GID = &gid
	}
	if payload.Size != nil {
		size := *payload.Size
		changes.Size = &size
	}
	if len(payload.XAttrs) > 0 {
		if changes.XAttrs == nil {
			changes.XAttrs = make(map[string][]byte, len(payload.XAttrs))
		}
		for k, v := range payload.XAttrs {
			changes.XAttrs[k] = []byte(v)
		}
	}
	if changes.Mode == nil && changes.UID == nil && changes.GID == nil && changes.Size == nil && len(changes.XAttrs) == 0 {
		http.Error(w, "no attributes to update", http.StatusBadRequest)
		return
	}
	if err := s.FS.SetAttr(ctx, p, changes, user); err != nil {
		httpError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) postFile(ctx context.Context, w http.ResponseWriter, r *http.Request, p string) {
	var payload fifoPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	switch strings.ToLower(payload.Type) {
	case "fifo":
		mode := os.FileMode(0o666)
		if payload.Mode != nil {
			mode = os.FileMode(*payload.Mode)
		}
		if err := s.FS.Mkfifo(ctx, p, mode, userFromRequest(r)); err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "unsupported type", http.StatusBadRequest)
	}
}

func userFromRequest(r *http.Request) vfs.User {
	var user vfs.User
	if uidStr := r.Header.Get("X-User-Uid"); uidStr != "" {
		if uid, err := strconv.ParseUint(uidStr, 10, 32); err == nil {
			user.UID = uint32(uid)
		}
	}
	if gidStr := r.Header.Get("X-User-Gid"); gidStr != "" {
		if gid, err := strconv.ParseUint(gidStr, 10, 32); err == nil {
			user.GID = uint32(gid)
		}
	}
	if groups := r.Header.Values("X-User-Group"); len(groups) > 0 {
		for _, g := range groups {
			if gid, err := strconv.ParseUint(g, 10, 32); err == nil {
				user.Supplementary = append(user.Supplementary, uint32(gid))
			}
		}
	}
	return user
}

type sequentialWriter struct {
	w   http.ResponseWriter
	pos int64
}

func (s *sequentialWriter) WriteAt(p []byte, off int64) (int, error) {
	if off != s.pos {
		return 0, fmt.Errorf("non-sequential write: off=%d pos=%d", off, s.pos)
	}
	n, err := s.w.Write(p)
	s.pos += int64(n)
	return n, err
}

type dirEntry struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Size int64  `json:"size,omitempty"`
}

func toDirEntry(entry fs.Entry) dirEntry {
	switch {
	case entry.Dir != nil:
		return dirEntry{Name: entry.Name, Type: "dir"}
	case entry.Object != nil:
		return dirEntry{Name: entry.Name, Type: "file", Size: entry.Object.Size()}
	case entry.Link != nil:
		return dirEntry{Name: entry.Name, Type: "link"}
	default:
		return dirEntry{Name: entry.Name, Type: "unknown"}
	}
}

func parseRangeHeader(header string, size int64) (int64, int64, error) {
	if size <= 0 {
		return 0, 0, fmt.Errorf("resource empty")
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range unit")
	}
	rangeSpec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if rangeSpec == "" || strings.Contains(rangeSpec, ",") {
		return 0, 0, fmt.Errorf("invalid range")
	}
	if strings.HasPrefix(rangeSpec, "-") {
		n, err := strconv.ParseInt(strings.TrimPrefix(rangeSpec, "-"), 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("invalid suffix range")
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, nil
	}
	parts := strings.SplitN(rangeSpec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range spec")
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("invalid range start")
	}
	var end int64
	if parts[1] == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || end < 0 {
			return 0, 0, fmt.Errorf("invalid range end")
		}
	}
	if start >= size {
		return 0, 0, fmt.Errorf("start beyond size")
	}
	if end >= size {
		end = size - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("start greater than end")
	}
	return start, end, nil
}

func (s *Server) listingParams(r *http.Request) (limit int, token string) {
	def, max := s.pageBounds()
	limit = def
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > max {
		limit = max
	}
	token = r.URL.Query().Get("page_token")
	return limit, token
}

func (s *Server) pageBounds() (def int, max int) {
	def = 100
	max = 1000
	if s.Opts.DefaultPageSize > 0 {
		def = s.Opts.DefaultPageSize
	}
	if s.Opts.MaxPageSize > 0 {
		max = s.Opts.MaxPageSize
	}
	if def > max {
		def = max
	}
	return def, max
}

func (s *Server) applyMiddleware(handler http.Handler) http.Handler {
	var chain []middleware.HTTPMiddleware
	if auth := middleware.APIKeyAuth(s.Opts.APIKey); auth != nil {
		chain = append(chain, auth)
	}
	if limit := middleware.RateLimit(s.Opts.RateLimit); limit != nil {
		chain = append(chain, limit)
	}
	if len(chain) == 0 {
		return handler
	}
	return middleware.Wrap(handler, chain...)
}

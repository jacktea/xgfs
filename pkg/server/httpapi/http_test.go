package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/vfs"
)

func TestHTTPAPIPutAndGet(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	handler := srv.router()
	req := httptest.NewRequest(http.MethodPut, "/files/test.txt", bytes.NewBufferString("hello"))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/files/test.txt", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "hello" {
		t.Fatalf("expected hello, got %q", string(body))
	}
}

func TestHTTPAPIRangeGet(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/range.txt", fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("hello world"), fs.IOOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	handler := srv.router()

	req := httptest.NewRequest(http.MethodGet, "/files/range.txt", nil)
	req.Header.Set("Range", "bytes=6-10")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "world" {
		t.Fatalf("expected world, got %q", string(body))
	}
	if got := rr.Header().Get("Content-Range"); got != "bytes 6-10/11" {
		t.Fatalf("unexpected content-range %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/files/range.txt", nil)
	req.Header.Set("Range", "bytes=50-60")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected 416, got %d", rr.Code)
	}
}

func TestHTTPAPIAuthMiddleware(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{}), Opts: Options{APIKey: "secret"}}
	handler := srv.router()
	req := httptest.NewRequest(http.MethodGet, "/dirs/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	req.Header.Set("Authorization", "Bearer secret")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 after auth, got %d", rr.Code)
	}
}

func TestHTTPAPIRateLimit(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	now := time.Unix(0, 0)
	opts := Options{
		RateLimit: middleware.RateLimitOptions{
			Requests: 1,
			Window:   time.Second,
			Now: func() time.Time {
				return now
			},
		},
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{}), Opts: opts}
	handler := srv.router()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected first request ok, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected rate limit, got %d", rr.Code)
	}
	now = now.Add(time.Second)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected request after refill ok, got %d", rr.Code)
	}
}

func TestHTTPAPIPagination(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	files := []string{"/a", "/b", "/c"}
	for _, name := range files {
		obj, err := backend.Create(ctx, name, fsCreateOpts())
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := obj.Write(ctx, bytes.NewBufferString(name), fs.IOOptions{}); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	handler := srv.router()
	req := httptest.NewRequest(http.MethodGet, "/dirs/?limit=2", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list page 1: %d", rr.Code)
	}
	var resp struct {
		Entries       []dirEntry `json:"entries"`
		NextPageToken string     `json:"next_page_token"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Entries) != 2 || resp.NextPageToken == "" {
		t.Fatalf("unexpected page response: %+v", resp)
	}
	req = httptest.NewRequest(http.MethodGet, "/dirs/?limit=2&page_token="+resp.NextPageToken, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list page 2: %d", rr.Code)
	}
	resp = struct {
		Entries       []dirEntry `json:"entries"`
		NextPageToken string     `json:"next_page_token"`
	}{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if len(resp.Entries) != 1 || resp.NextPageToken != "" {
		t.Fatalf("expected final page, got %+v", resp)
	}
}

func TestHTTPAPIPatchRename(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/old.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("data"), fs.IOOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	payload := `{"rename_to":"/new.txt"}`
	req := httptest.NewRequest(http.MethodPatch, "/files/old.txt", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	srv.handleFiles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("patch status %d", rr.Code)
	}
	if _, err := backend.Stat(ctx, "/old.txt"); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected old path missing, err=%v", err)
	}
	if _, err := backend.Stat(ctx, "/new.txt"); err != nil {
		t.Fatalf("new path stat: %v", err)
	}
}

func TestHTTPAPIPatchChmod(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	obj, err := backend.Create(ctx, "/perm.txt", fsCreateOpts())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := obj.Write(ctx, bytes.NewBufferString("data"), fs.IOOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	payload := fmt.Sprintf(`{"mode":%d}`, 0o600)
	req := httptest.NewRequest(http.MethodPatch, "/files/perm.txt", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	srv.handleFiles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("patch status %d", rr.Code)
	}
	inode, err := backend.Stat(ctx, "/perm.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if inode.Metadata().Mode != 0o600 {
		t.Fatalf("expected mode 0600 got %o", inode.Metadata().Mode)
	}
}

func TestHTTPAPIMkfifo(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: vfs.New(backend, vfs.Options{})}
	payload := fmt.Sprintf(`{"type":"fifo","mode":%d}`, 0o770)
	req := httptest.NewRequest(http.MethodPost, "/files/fifo.pipe", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	srv.handleFiles(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("mkfifo status %d", rr.Code)
	}
	inode, err := backend.Stat(ctx, "/fifo.pipe")
	if err != nil {
		t.Fatalf("stat fifo: %v", err)
	}
	if inode.Metadata().Mode != 0o770 {
		t.Fatalf("expected mode 0770 got %o", inode.Metadata().Mode)
	}
}

func fsCreateOpts() fs.CreateOptions {
	return fs.CreateOptions{Mode: 0o644, Overwrite: true}
}

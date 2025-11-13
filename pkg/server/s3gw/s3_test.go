package s3gw

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
)

func TestS3GatewayPutGet(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: backend, Opt: Options{Bucket: "test"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/hello.txt", bytes.NewBufferString("world"))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/hello.txt", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "world" {
		t.Fatalf("expected world got %q", string(body))
	}
}

func TestS3GatewayAuthMiddleware(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: backend, Opt: Options{Bucket: "test", APIKey: "secret"}}
	req := httptest.NewRequest(http.MethodGet, "/?list-type=2", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	req.Header.Set("X-API-Key", "secret")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 after auth, got %d", rr.Code)
	}
}

func TestS3GatewayRateLimit(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	now := time.Unix(0, 0)
	srv := &Server{
		FS: backend,
		Opt: Options{
			Bucket: "test",
			RateLimit: middleware.RateLimitOptions{
				Requests: 1,
				Window:   time.Second,
				Now: func() time.Time {
					return now
				},
			},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/?list-type=2", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rr.Code)
	}
	now = now.Add(time.Second)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected ok after refill, got %d", rr.Code)
	}
}

func TestS3GatewayPagination(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: backend, Opt: Options{Bucket: "test"}}
	put := func(name string) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/"+name, bytes.NewBufferString(name))
		srv.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("put %s: %d", name, rr.Code)
		}
	}
	put("a.txt")
	put("b.txt")
	put("c.txt")
	req := httptest.NewRequest(http.MethodGet, "/?list-type=2&max-keys=2", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list page1: %d", rr.Code)
	}
	var resp listResult
	if err := xml.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode page1: %v", err)
	}
	if !resp.IsTruncated || resp.NextContinuationToken == "" {
		t.Fatalf("expected truncation: %+v", resp)
	}
	req = httptest.NewRequest(http.MethodGet, "/?list-type=2&max-keys=2&continuation-token="+resp.NextContinuationToken, nil)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list page2: %d", rr.Code)
	}
	if err := xml.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if resp.IsTruncated {
		t.Fatalf("expected final page, got truncated")
	}
}

func TestS3GatewayPosixMetadata(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: backend, Opt: Options{Bucket: "test"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/meta.txt", bytes.NewBufferString("posix"))
	req.Header.Set("X-Amz-Meta-Posix-Mode", "600")
	req.Header.Set("X-Amz-Meta-Posix-Uid", "2000")
	req.Header.Set("X-Amz-Meta-Posix-Gid", "3000")
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put metadata: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/meta.txt", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get metadata: %d", rr.Code)
	}
	if got := rr.Header().Get("X-Amz-Meta-Posix-Mode"); got != "600" {
		t.Fatalf("expected mode header 600 got %s", got)
	}
	if got := rr.Header().Get("X-Amz-Meta-Posix-Uid"); got != "2000" {
		t.Fatalf("expected uid 2000 got %s", got)
	}
	if got := rr.Header().Get("X-Amz-Meta-Posix-Gid"); got != "3000" {
		t.Fatalf("expected gid 3000 got %s", got)
	}
}

func TestS3GatewayRename(t *testing.T) {
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	srv := &Server{FS: backend, Opt: Options{Bucket: "test"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/old.txt", bytes.NewBufferString("data"))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put old: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/old.txt?rename=/new.txt", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("rename status %d", rr.Code)
	}
	if _, err := backend.Stat(ctx, "/new.txt"); err != nil {
		t.Fatalf("stat new: %v", err)
	}
	if _, err := backend.Stat(ctx, "/old.txt"); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected old missing, err=%v", err)
	}
}

type listResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
}

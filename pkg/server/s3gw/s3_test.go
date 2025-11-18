package s3gw

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/localfs"
	"github.com/jacktea/xgfs/pkg/server/middleware"
	"github.com/jacktea/xgfs/pkg/vfs"
)

func newTestServer(t *testing.T, opt Options) *Server {
	t.Helper()
	ctx := context.Background()
	backend, err := localfs.New(ctx, localfs.Config{BlobRoot: t.TempDir(), CacheEntries: 8})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	return &Server{FS: vfs.New(backend, vfs.Options{}), Opt: opt}
}

func createBucket(t *testing.T, srv *Server, name string, mutate ...func(*http.Request)) {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/"+name, nil)
	for _, fn := range mutate {
		fn(req)
	}
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("create bucket %s: %d", name, rr.Code)
	}
}

func putObject(t *testing.T, srv *Server, bucket, key, body string) {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/%s/%s", bucket, key), bytes.NewBufferString(body))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put %s/%s: %d", bucket, key, rr.Code)
	}
}

func TestS3GatewayPutGet(t *testing.T) {
	srv := newTestServer(t, Options{})
	createBucket(t, srv, "test")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/test/hello.txt", bytes.NewBufferString("world"))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test/hello.txt", nil)
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
	srv := newTestServer(t, Options{APIKey: "secret"})
	createBucket(t, srv, "test", func(r *http.Request) {
		r.Header.Set("X-API-Key", "secret")
	})
	req := httptest.NewRequest(http.MethodGet, "/test?list-type=2", nil)
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
	now := time.Unix(0, 0)
	srv := newTestServer(t, Options{
		RateLimit: middleware.RateLimitOptions{
			Requests: 1,
			Window:   time.Second,
			Now: func() time.Time {
				return now
			},
		},
	})
	if _, err := srv.FS.Mkdir(context.Background(), "/test", fs.MkdirOptions{Parents: true, Mode: 0o755}); err != nil && !errors.Is(err, fs.ErrAlreadyExist) {
		t.Fatalf("prepare bucket: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/test?list-type=2", nil)
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
	srv := newTestServer(t, Options{})
	createBucket(t, srv, "test")
	putObject(t, srv, "test", "a.txt", "a.txt")
	putObject(t, srv, "test", "b.txt", "b.txt")
	putObject(t, srv, "test", "c.txt", "c.txt")
	req := httptest.NewRequest(http.MethodGet, "/test?list-type=2&max-keys=2", nil)
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
	req = httptest.NewRequest(http.MethodGet, "/test?list-type=2&max-keys=2&continuation-token="+resp.NextContinuationToken, nil)
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
	srv := newTestServer(t, Options{})
	createBucket(t, srv, "test")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/test/meta.txt", bytes.NewBufferString("posix"))
	req.Header.Set("X-Amz-Meta-Posix-Mode", "600")
	req.Header.Set("X-Amz-Meta-Posix-Uid", "2000")
	req.Header.Set("X-Amz-Meta-Posix-Gid", "3000")
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put metadata: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test/meta.txt", nil)
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
	srv := &Server{FS: vfs.New(backend, vfs.Options{}), Opt: Options{}}
	createBucket(t, srv, "test")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/test/old.txt", bytes.NewBufferString("data"))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put old: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/test/old.txt?rename=/test/new.txt", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("rename status %d", rr.Code)
	}
	if _, err := backend.Stat(ctx, "/test/new.txt"); err != nil {
		t.Fatalf("stat new: %v", err)
	}
	if _, err := backend.Stat(ctx, "/test/old.txt"); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("expected old missing, err=%v", err)
	}
}

func TestS3GatewayBucketLifecycle(t *testing.T) {
	srv := newTestServer(t, Options{})
	createBucket(t, srv, "alpha")
	createBucket(t, srv, "beta")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list buckets: %d", rr.Code)
	}
	var result gofakes3.Storage
	if err := xml.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("decode buckets: %v", err)
	}
	names := result.Buckets.Names()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("unexpected bucket list: %v", names)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/alpha", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK && rr.Code != http.StatusNoContent {
		t.Fatalf("delete bucket: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/alpha", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected not found deleting missing bucket, got %d", rr.Code)
	}
}

func TestS3GatewayMultipartUpload(t *testing.T) {
	srv := newTestServer(t, Options{})
	createBucket(t, srv, "multi")

	initReq := httptest.NewRequest(http.MethodPost, "/multi/object.txt?uploads", nil)
	initRR := httptest.NewRecorder()
	srv.ServeHTTP(initRR, initReq)
	if initRR.Code != http.StatusOK {
		t.Fatalf("initiate upload: %d", initRR.Code)
	}
	var initResp gofakes3.InitiateMultipartUploadResult
	if err := xml.NewDecoder(initRR.Body).Decode(&initResp); err != nil {
		t.Fatalf("decode initiate: %v", err)
	}
	etags := map[int]string{}
	uploadPart := func(part int, body string) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/multi/object.txt?partNumber=%d&uploadId=%s", part, initResp.UploadID), bytes.NewBufferString(body))
		srv.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("upload part %d: %d", part, rr.Code)
		}
		etags[part] = rr.Header().Get("ETag")
	}
	uploadPart(1, "hello ")
	uploadPart(2, "world")

	compReq := gofakes3.CompleteMultipartUploadRequest{
		Parts: []gofakes3.CompletedPart{
			{PartNumber: 1, ETag: etags[1]},
			{PartNumber: 2, ETag: etags[2]},
		},
	}
	buf := &bytes.Buffer{}
	if err := xml.NewEncoder(buf).Encode(compReq); err != nil {
		t.Fatalf("encode complete: %v", err)
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/multi/object.txt?uploadId=%s", initResp.UploadID), bytes.NewReader(buf.Bytes()))
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("complete upload: %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/multi/object.txt", nil)
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get multipart object: %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "hello world" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

type listResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
}

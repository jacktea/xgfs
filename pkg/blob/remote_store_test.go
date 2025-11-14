package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jacktea/xgfs/pkg/encryption"
)

type noopSigner struct{}

func (n *noopSigner) Sign(req *http.Request, payloadHash string) error { return nil }

func TestRemoteStorePutDeduplicates(t *testing.T) {
	ctx := context.Background()
	headCount := 0
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCount++
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	store, err := NewRemoteStore(RemoteConfig{Endpoint: server.URL, Bucket: "bucket", Client: server.Client()}, &noopSigner{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	id, _, err := store.Put(ctx, strings.NewReader("hello"), 5, PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	sum := sha256.Sum256([]byte("hello"))
	expect := ID(hex.EncodeToString(sum[:]))
	if id != expect {
		t.Fatalf("expected id %s got %s", expect, id)
	}
	if headCount != 1 {
		t.Fatalf("expected one HEAD, got %d", headCount)
	}
}

func TestRemoteStorePutUploads(t *testing.T) {
	ctx := context.Background()
	var (
		headCount int
		putCount  int
		lastBody  []byte
	)
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCount++
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			putCount++
			body, _ := io.ReadAll(r.Body)
			lastBody = body
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	store, err := NewRemoteStore(RemoteConfig{Endpoint: server.URL, Bucket: "bucket", Client: server.Client()}, &noopSigner{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, _, err := store.Put(ctx, strings.NewReader("payload"), 7, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if headCount != 1 || putCount != 1 {
		t.Fatalf("expected one head and one put, got %d head %d put", headCount, putCount)
	}
	if string(lastBody) != "payload" {
		t.Fatalf("expected payload body, got %q", string(lastBody))
	}
}

func TestS3SignerAddsAuthorization(t *testing.T) {
	signer := &s3Signer{
		accessKey: "AKIAEXAMPLE",
		secretKey: "SECRET",
		region:    "us-east-1",
		now: func() time.Time {
			return time.Date(2023, 3, 10, 12, 0, 0, 0, time.UTC)
		},
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com/bucket/object", nil)
	req.Header.Set("x-amz-content-sha256", emptyPayloadHash())
	if err := signer.Sign(req, emptyPayloadHash()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		t.Fatalf("expected aws signature, got %s", auth)
	}
}
func TestRemoteStoreEncryptedPayload(t *testing.T) {
	ctx := context.Background()
	var body []byte
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			body, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	store, err := NewRemoteStore(RemoteConfig{Endpoint: server.URL, Bucket: "bucket", Client: server.Client()}, &noopSigner{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := bytes.Repeat([]byte("k"), 32)
	opts := PutOptions{Encryption: encryption.Options{Method: encryption.MethodAES256CTR, Key: key}}
	if _, _, err := store.Put(ctx, strings.NewReader("secret"), 6, opts); err != nil {
		t.Fatalf("put: %v", err)
	}
	expectedLen := len("secret") + encryption.Overhead(encryption.MethodAES256CTR)
	if len(body) != expectedLen {
		t.Fatalf("expected encrypted payload length %d got %d", expectedLen, len(body))
	}
	if string(body[encryption.Overhead(encryption.MethodAES256CTR):]) == "secret" {
		t.Fatalf("encryption did not modify payload")
	}
}

func TestOSSSignerAddsAuthorization(t *testing.T) {
	signer := &ossSigner{accessKey: "ak", secretKey: "sk"}
	req, _ := http.NewRequest(http.MethodPut, "https://oss.example.com/bucket/object", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-MD5", "d41d8cd98f00b204e9800998ecf8427e")
	if err := signer.Sign(req, ""); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !strings.HasPrefix(req.Header.Get("Authorization"), "OSS ak:") {
		t.Fatalf("expected OSS authorization header")
	}
}

func TestCOSSignerAddsAuthorization(t *testing.T) {
	signer := &cosSigner{accessKey: "ak", secretKey: "sk", now: func() time.Time { return time.Unix(1700000000, 0) }}
	req, _ := http.NewRequest(http.MethodGet, "https://cos.example.com/bucket/object", nil)
	if err := signer.Sign(req, ""); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !strings.Contains(req.Header.Get("Authorization"), "q-sign-algorithm=sha1") {
		t.Fatalf("expected cos authorization header")
	}
}

func TestRemoteStoreCachesShards(t *testing.T) {
	ctx := context.Background()
	var (
		stored []byte
		gets   int
	)
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			stored = append([]byte(nil), body...)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			gets++
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write(stored)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	store, err := NewRemoteStore(RemoteConfig{Endpoint: server.URL, Bucket: "bucket", Client: server.Client()}, &noopSigner{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	id, _, err := store.Put(ctx, strings.NewReader("cache-me"), 8, PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 2; i++ {
		rc, _, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		rc.Close()
	}
	if gets != 1 {
		t.Fatalf("expected single remote GET, got %d", gets)
	}
}

func newHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("httptest listener unavailable: %v", err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener = ln
	srv.Start()
	return srv
}

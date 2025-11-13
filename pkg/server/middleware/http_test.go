package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAPIKeyAuth(t *testing.T) {
	protected := APIKeyAuth("secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	protected.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	req.Header.Set("X-API-Key", "secret")
	rr = httptest.NewRecorder()
	protected.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	current := time.Unix(0, 0)
	opts := RateLimitOptions{
		Requests: 1,
		Window:   time.Second,
		Now: func() time.Time {
			return current
		},
	}
	limited := RateLimit(opts)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	limited.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected first request allowed, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	limited.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request blocked, got %d", rr.Code)
	}
	current = current.Add(time.Second)
	rr = httptest.NewRecorder()
	limited.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected request allowed after refill, got %d", rr.Code)
	}
}

package middleware

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPMiddleware wraps an http.Handler.
type HTTPMiddleware func(http.Handler) http.Handler

// Wrap applies middleware in order.
func Wrap(h http.Handler, middlewares ...HTTPMiddleware) http.Handler {
	chain := chain(middlewares...)
	return chain(h)
}

func chain(middlewares ...HTTPMiddleware) HTTPMiddleware {
	filtered := make([]HTTPMiddleware, 0, len(middlewares))
	for _, mw := range middlewares {
		if mw != nil {
			filtered = append(filtered, mw)
		}
	}
	return func(next http.Handler) http.Handler {
		handler := next
		for i := len(filtered) - 1; i >= 0; i-- {
			handler = filtered[i](handler)
		}
		return handler
	}
}

// APIKeyAuth enforces a shared secret sent via X-API-Key or Bearer token.
func APIKeyAuth(key string) HTTPMiddleware {
	if strings.TrimSpace(key) == "" {
		return nil
	}
	secret := strings.TrimSpace(key)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if extractAPIKey(r) != secret {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractAPIKey(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-API-Key")); v != "" {
		return v
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

// RateLimitOptions configures the shared rate limiter.
type RateLimitOptions struct {
	Requests int
	Window   time.Duration
	Now      func() time.Time
}

// RateLimit enforces a token bucket over all requests.
func RateLimit(opts RateLimitOptions) HTTPMiddleware {
	if opts.Requests <= 0 || opts.Window <= 0 {
		return nil
	}
	bucket := newTokenBucket(opts)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !bucket.Allow() {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type tokenBucket struct {
	mu           sync.Mutex
	capacity     float64
	tokens       float64
	refillPerSec float64
	last         time.Time
	now          func() time.Time
}

func newTokenBucket(opts RateLimitOptions) *tokenBucket {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &tokenBucket{
		capacity:     float64(opts.Requests),
		tokens:       float64(opts.Requests),
		refillPerSec: float64(opts.Requests) / opts.Window.Seconds(),
		last:         now(),
		now:          now,
	}
}

func (t *tokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	elapsed := now.Sub(t.last).Seconds()
	if elapsed > 0 {
		t.tokens += elapsed * t.refillPerSec
		if t.tokens > t.capacity {
			t.tokens = t.capacity
		}
		t.last = now
	}
	if t.tokens < 1 {
		return false
	}
	t.tokens--
	return true
}

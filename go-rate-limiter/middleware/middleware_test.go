package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Divyesh-Shah-17/Rate-Limiter/go-rate-limiter/limiter"
)

// mockLimiter implements limiter.RateLimiter for testing purposes.
type mockLimiter struct {
	allowFunc func(ctx context.Context, key string, limit int, window time.Duration) (limiter.Result, error)
}

func (m *mockLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (limiter.Result, error) {
	return m.allowFunc(ctx, key, limit, window)
}

func TestRateLimiterMiddleware_Allowed(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, key string, limit int, window time.Duration) (limiter.Result, error) {
			return limiter.Result{
				Allowed:   true,
				Remaining: 4,
				Reset:     5 * time.Second,
			}, nil
		},
	}

	mw := NewRateLimiterMiddleware(Config{
		Limiter: mock,
		Limit:   5,
		Window:  10 * time.Second,
	})

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if rec.Header().Get("X-RateLimit-Limit") != "5" {
		t.Errorf("unexpected limit header: %s", rec.Header().Get("X-RateLimit-Limit"))
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "4" {
		t.Errorf("unexpected remaining header: %s", rec.Header().Get("X-RateLimit-Remaining"))
	}
	if rec.Header().Get("X-RateLimit-Reset") != "5.00" {
		t.Errorf("unexpected reset header: %s", rec.Header().Get("X-RateLimit-Reset"))
	}
}

func TestRateLimiterMiddleware_Blocked(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, key string, limit int, window time.Duration) (limiter.Result, error) {
			return limiter.Result{
				Allowed:   false,
				Remaining: 0,
				Reset:     3 * time.Second,
			}, nil
		},
	}

	mw := NewRateLimiterMiddleware(Config{
		Limiter: mock,
		Limit:   5,
		Window:  10 * time.Second,
	})

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", rec.Code)
	}

	if rec.Header().Get("Retry-After") != "3" {
		t.Errorf("unexpected Retry-After header: %s", rec.Header().Get("Retry-After"))
	}
}

func TestRateLimiterMiddleware_Errors(t *testing.T) {
	mock := &mockLimiter{
		allowFunc: func(ctx context.Context, key string, limit int, window time.Duration) (limiter.Result, error) {
			return limiter.Result{}, errors.New("redis down")
		},
	}

	t.Run("Fail Closed", func(t *testing.T) {
		mw := NewRateLimiterMiddleware(Config{
			Limiter:  mock,
			Limit:    5,
			Window:   10 * time.Second,
			FailOpen: false,
		})

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("expected status 500, got %d", rec.Code)
		}
	})

	t.Run("Fail Open", func(t *testing.T) {
		mw := NewRateLimiterMiddleware(Config{
			Limiter:  mock,
			Limit:    5,
			Window:   10 * time.Second,
			FailOpen: true,
		})

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}))

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
	})
}

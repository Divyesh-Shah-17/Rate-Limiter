package middleware

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Divyesh-Shah-17/Rate-Limiter/go-rate-limiter/limiter"
)

// KeyFunc extracts a rate-limiting key from the incoming HTTP request.
type KeyFunc func(r *http.Request) string

// DefaultKeyFunc extracts the client IP address from proxy headers or remote address.
func DefaultKeyFunc(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return ip
	}
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	return r.RemoteAddr
}

// Config configures the rate limiting middleware.
type Config struct {
	Limiter  limiter.RateLimiter
	KeyFunc  KeyFunc
	Limit    int
	Window   time.Duration
	FailOpen bool // If true, allow requests to proceed if the rate limiter backend (Redis) errors out.
}

// NewRateLimiterMiddleware creates a net/http middleware that enforces rate limiting.
func NewRateLimiterMiddleware(cfg Config) func(http.Handler) http.Handler {
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = DefaultKeyFunc
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.KeyFunc(r)

			res, err := cfg.Limiter.Allow(r.Context(), key, cfg.Limit, cfg.Window)
			if err != nil {
				// Log the backend rate limiter error
				log.Printf("Rate limiter error for key %q: %v", key, err)

				if cfg.FailOpen {
					// Proceed with request (fail-open)
					next.ServeHTTP(w, r)
					return
				}

				// Reject request (fail-closed)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// Apply standard rate limit response headers
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(cfg.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatFloat(res.Reset.Seconds(), 'f', 2, 64))

			if !res.Allowed {
				// Retry-After is formatted in whole seconds
				retrySec := int(res.Reset.Seconds())
				if retrySec < 1 {
					retrySec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

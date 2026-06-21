package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/Divyesh-Shah-17/Rate-Limiter/go-rate-limiter/limiter"
	"github.com/Divyesh-Shah-17/Rate-Limiter/go-rate-limiter/middleware"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	log.Printf("Connecting to Redis at %s...", redisAddr)
	rClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})

	// Check Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v. Rate limiter will operate with fail-open logic.", err)
	} else {
		log.Println("Successfully connected to Redis!")
	}

	// Instantiate the Rate Limiter with L1 Cache enabled
	rateLimiter := limiter.NewRedisRateLimiter(rClient, limiter.Config{
		EnableL1Cache:   true,
		L1ShardCount:    64,
		L1MaxTTL:        2 * time.Second, // Cache throttled keys locally for up to 2 seconds
		L1CleanupPeriod: 10 * time.Second,
	})
	defer rateLimiter.Close()

	// Set up request router
	mux := http.NewServeMux()

	// Configure the rate limiter middleware: 5 requests per 10 seconds
	limiterCfg := middleware.Config{
		Limiter:  rateLimiter,
		Limit:    5,
		Window:   10 * time.Second,
		FailOpen: true, // Request proceeds if Redis backend fails
	}
	rateLimitedMiddleware := middleware.NewRateLimiterMiddleware(limiterCfg)

	// Register routes
	mux.Handle("/", rateLimitedMiddleware(http.HandlerFunc(homeHandler)))
	mux.HandleFunc("/unlimited", unlimitedHandler)
	mux.HandleFunc("/health", healthHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      requestLogger(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	log.Printf("Server listening on http://localhost:%s", port)
	log.Printf("-> Rate limited endpoint: http://localhost:%s/ (5 requests per 10 seconds)", port)
	log.Printf("-> Unlimited endpoint:    http://localhost:%s/unlimited", port)
	log.Printf("-> Health endpoint:       http://localhost:%s/health", port)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "Welcome to the Rate-Limited Home Page!")
}

func unlimitedHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "Welcome to the Unlimited Page! No restrictions here.")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "OK")
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s - %v", r.Method, r.RequestURI, r.RemoteAddr, time.Since(start))
	})
}

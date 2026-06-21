package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisRateLimiter_Basic(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: false,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "test_client"
	limit := 3
	window := 10 * time.Second

	// 1st Request - Allowed
	res, err := lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed || res.Remaining != 2 {
		t.Fatalf("expected allowed with 2 remaining, got allowed=%t, remaining=%d", res.Allowed, res.Remaining)
	}

	// 2nd Request - Allowed
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed || res.Remaining != 1 {
		t.Fatalf("expected allowed with 1 remaining, got allowed=%t, remaining=%d", res.Allowed, res.Remaining)
	}

	// 3rd Request - Allowed
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed || res.Remaining != 0 {
		t.Fatalf("expected allowed with 0 remaining, got allowed=%t, remaining=%d", res.Allowed, res.Remaining)
	}

	// 4th Request - Blocked
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected blocked, got allowed")
	}
}

func TestRedisRateLimiter_Reset(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: false,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "reset_client"
	limit := 1
	window := 50 * time.Millisecond

	// 1st request - allowed
	res, err := lim.Allow(ctx, key, limit, window)
	if err != nil || !res.Allowed {
		t.Fatalf("expected allowed: %v", err)
	}

	// 2nd request - blocked
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil || res.Allowed {
		t.Fatalf("expected blocked: %v", err)
	}

	// Wait for reset (longer than window)
	time.Sleep(2 * window)

	// 3rd request - allowed again
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected allowed after reset, got blocked")
	}
}

func TestRedisRateLimiter_L1CacheShortCircuit(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	// Enable L1 cache with a 500ms max TTL
	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: true,
		L1ShardCount:  4,
		L1MaxTTL:      500 * time.Millisecond,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "l1_test_client"
	limit := 1
	window := 10 * time.Second // Use 10s to ensure both requests fall into the same bucket

	// First request: hits Redis, returns Allowed
	res, err := lim.Allow(ctx, key, limit, window)
	if err != nil || !res.Allowed {
		t.Fatalf("expected first request to be allowed: %v", err)
	}

	// Second request: hits Redis, gets Blocked, sets L1 cache block
	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil || res.Allowed {
		t.Fatalf("expected second request to be blocked: %v", err)
	}

	// Reset miniredis to see if L1 cache actually short-circuits.
	// If L1 cache works, the next request will be blocked WITHOUT querying Redis.
	// We close the miniredis server to simulate a Redis failure or disconnect.
	// Subsequent requests should still be blocked by L1 cache instantly without returning an error.
	s.Close()

	res, err = lim.Allow(ctx, key, limit, window)
	if err != nil {
		t.Fatalf("expected L1 short circuit to handle call without error, got: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected L1 cache to block request")
	}
}

func TestRedisRateLimiter_Concurrency(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: false,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "concurrent_client"
	limit := 100
	window := 10 * time.Second

	var wg sync.WaitGroup
	workers := 10
	requestsPerWorker := 20

	var allowedCount int64
	var blockedCount int64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				res, err := lim.Allow(ctx, key, limit, window)
				if err != nil {
					t.Errorf("limiter allow error: %v", err)
					return
				}
				if res.Allowed {
					atomic.AddInt64(&allowedCount, 1)
				} else {
					atomic.AddInt64(&blockedCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	total := allowedCount + blockedCount
	expectedTotal := int64(workers * requestsPerWorker)
	if total != expectedTotal {
		t.Fatalf("expected total requests %d, got %d", expectedTotal, total)
	}

	// We should allow exactly `limit` requests since window is long
	if allowedCount != int64(limit) {
		t.Fatalf("expected exactly %d allowed requests, got %d", limit, allowedCount)
	}
}

// BenchmarkRedisRateLimiter_L1Disabled measures performance without L1 cache (all hit Redis).
func BenchmarkRedisRateLimiter_L1Disabled(b *testing.B) {
	s, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: false,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "benchmark_client"
	limit := 1000
	window := 10 * time.Second

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = lim.Allow(ctx, key, limit, window)
		}
	})
}

// BenchmarkRedisRateLimiter_L1Enabled measures performance with L1 cache under heavy throttling.
func BenchmarkRedisRateLimiter_L1Enabled(b *testing.B) {
	s, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to run miniredis: %v", err)
	}
	defer s.Close()

	rClient := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rClient.Close()

	lim := NewRedisRateLimiter(rClient, Config{
		EnableL1Cache: true,
		L1ShardCount:  64,
		L1MaxTTL:      1 * time.Second,
	})
	defer lim.Close()

	ctx := context.Background()
	key := "benchmark_client"
	limit := 10 // small limit to trigger throttling immediately
	window := 10 * time.Second

	// Trigger initial throttling so L1 cache kicks in
	for i := 0; i < 20; i++ {
		_, _ = lim.Allow(ctx, key, limit, window)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = lim.Allow(ctx, key, limit, window)
		}
	})
}

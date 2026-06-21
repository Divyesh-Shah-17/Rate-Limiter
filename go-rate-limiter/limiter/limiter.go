package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter defines the contract for checking rate limits.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error)
}

// Result holds the metadata of a rate limit check.
type Result struct {
	Allowed   bool          // True if the request is permitted
	Remaining int           // Remaining capacity in the current sliding window
	Reset     time.Duration // Time remaining until the capacity resets/starts freeing up
}

// Config specifies configuration options for the rate limiter.
type Config struct {
	EnableL1Cache   bool          // If true, L1 in-memory cache will be used for short-circuiting
	L1ShardCount    int           // Number of shards for L1 Cache (must be power of 2)
	L1MaxTTL        time.Duration // Maximum block duration in L1 (defaults to 1s)
	L1CleanupPeriod time.Duration // Interval at which expired L1 entries are swept (defaults to 10s)
}

// RedisRateLimiter implements the RateLimiter interface using Redis and an optional L1 cache.
type RedisRateLimiter struct {
	client  redis.UniversalClient
	l1Cache *L1Cache
	cfg     Config
}

// NewRedisRateLimiter instantiates a new RedisRateLimiter with the given client and configuration.
func NewRedisRateLimiter(client redis.UniversalClient, cfg Config) *RedisRateLimiter {
	var l1 *L1Cache
	if cfg.EnableL1Cache {
		if cfg.L1ShardCount <= 0 {
			cfg.L1ShardCount = 64
		}
		if cfg.L1MaxTTL <= 0 {
			cfg.L1MaxTTL = 1 * time.Second
		}
		if cfg.L1CleanupPeriod <= 0 {
			cfg.L1CleanupPeriod = 10 * time.Second
		}
		l1 = NewL1Cache(cfg.L1ShardCount)
		l1.StartCleanup(cfg.L1CleanupPeriod)
	}

	return &RedisRateLimiter{
		client:  client,
		l1Cache: l1,
		cfg:     cfg,
	}
}

// Close releases resources held by the limiter (e.g. L1 cache cleanup goroutine).
func (r *RedisRateLimiter) Close() {
	if r.l1Cache != nil {
		r.l1Cache.Close()
	}
}

// Allow checks if a request is allowed under the rate limit for the given key.
// It returns whether the request is allowed, the remaining capacity, and the reset duration.
func (r *RedisRateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	if limit <= 0 {
		return Result{Allowed: false, Remaining: 0, Reset: 0}, fmt.Errorf("limit must be positive")
	}
	if window <= 0 {
		return Result{Allowed: false, Remaining: 0, Reset: 0}, fmt.Errorf("window must be positive")
	}

	// 1. Local L1 Fast-Path Check
	if r.cfg.EnableL1Cache && r.l1Cache != nil {
		if remainingBlock, ok := r.l1Cache.GetBlockedRemaining(key); ok {
			return Result{
				Allowed:   false,
				Remaining: 0,
				Reset:     remainingBlock,
			}, nil
		}
	}

	// 2. Calculate current and previous buckets for Sliding Window
	now := time.Now()
	nowMs := now.UnixNano() / int64(time.Millisecond)
	windowMs := int64(window / time.Millisecond)

	currentBucketMs := (nowMs / windowMs) * windowMs
	previousBucketMs := currentBucketMs - windowMs

	// Use Redis Hash Tags to ensure all keys hash to the same cluster slot
	currentKey := fmt.Sprintf("{%s}:%d", key, currentBucketMs)
	previousKey := fmt.Sprintf("{%s}:%d", key, previousBucketMs)

	// 3. Execute Lua script in Redis
	res, err := slidingWindowScript.Run(ctx, r.client, []string{currentKey, previousKey}, limit, windowMs, nowMs).Result()
	if err != nil {
		return Result{Allowed: false, Remaining: 0, Reset: 0}, fmt.Errorf("redis script execution error: %w", err)
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) < 3 {
		return Result{Allowed: false, Remaining: 0, Reset: 0}, fmt.Errorf("unexpected script response type: %T", res)
	}

	allowedVal, ok1 := arr[0].(int64)
	remainingVal, ok2 := arr[1].(int64)
	resetMsVal, ok3 := arr[2].(int64)

	if !ok1 || !ok2 || !ok3 {
		return Result{Allowed: false, Remaining: 0, Reset: 0}, fmt.Errorf("unexpected type inside script response: %v", arr)
	}

	allowed := allowedVal == 1
	remaining := int(remainingVal)
	resetDuration := time.Duration(resetMsVal) * time.Millisecond

	// 4. Update L1 Cache if request is blocked
	if !allowed && r.cfg.EnableL1Cache && r.l1Cache != nil {
		// Limit the local block TTL to avoid over-blocking
		l1TTL := r.cfg.L1MaxTTL
		if resetDuration < l1TTL {
			l1TTL = resetDuration
		}
		r.l1Cache.Block(key, l1TTL)
	}

	return Result{
		Allowed:   allowed,
		Remaining: remaining,
		Reset:     resetDuration,
	}, nil
}

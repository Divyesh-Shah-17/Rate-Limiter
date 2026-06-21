package limiter

import (
	"sync"
	"time"
)

// FNV-1a hash prime and offset basis constants
const (
	prime32  = 16777619
	offset32 = 2166136261
)

// fnv32 hashes a string key to a 32-bit unsigned integer.
func fnv32(key string) uint32 {
	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// shard holds a localized map of throttled keys and a dedicated mutex to minimize lock contention.
type shard struct {
	mu      sync.RWMutex
	blocked map[string]time.Time
}

// L1Cache is a highly concurrent in-memory cache used as a local fast-path to throttle keys.
type L1Cache struct {
	shards    []*shard
	shardMask uint32
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewL1Cache creates a new L1Cache with the specified number of shards.
// shardCount must be a power of 2 (e.g. 16, 32, 64, 128) for fast bitwise addressing.
// If shardCount is not a power of 2, it will default to 64.
func NewL1Cache(shardCount int) *L1Cache {
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		shardCount = 64
	}

	shards := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &shard{
			blocked: make(map[string]time.Time),
		}
	}

	return &L1Cache{
		shards:    shards,
		shardMask: uint32(shardCount - 1),
		stopChan:  make(chan struct{}),
	}
}

// GetBlockedRemaining returns the remaining blocked duration for a key.
// If the key is not blocked, or its block period has expired, it returns 0 and false.
func (c *L1Cache) GetBlockedRemaining(key string) (time.Duration, bool) {
	hash := fnv32(key)
	shardIdx := hash & c.shardMask
	s := c.shards[shardIdx]

	s.mu.RLock()
	blockedUntil, exists := s.blocked[key]
	s.mu.RUnlock()

	if !exists {
		return 0, false
	}

	now := time.Now()
	if now.After(blockedUntil) {
		// Lazy eviction
		s.mu.Lock()
		// Recheck in case it was deleted/updated concurrently
		if currentVal, ok := s.blocked[key]; ok && now.After(currentVal) {
			delete(s.blocked, key)
		}
		s.mu.Unlock()
		return 0, false
	}

	return blockedUntil.Sub(now), true
}

// Block marks a key as throttled until time.Now() + duration.
func (c *L1Cache) Block(key string, duration time.Duration) {
	if duration <= 0 {
		return
	}

	hash := fnv32(key)
	shardIdx := hash & c.shardMask
	s := c.shards[shardIdx]

	s.mu.Lock()
	s.blocked[key] = time.Now().Add(duration)
	s.mu.Unlock()
}

// StartCleanup starts a background goroutine that periodically removes expired entries.
func (c *L1Cache) StartCleanup(interval time.Duration) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.cleanup()
			case <-c.stopChan:
				return
			}
		}
	}()
}

// Close stops the periodic cleanup goroutine and blocks until it exits.
func (c *L1Cache) Close() {
	close(c.stopChan)
	c.wg.Wait()
}

// cleanup performs a sweep over all shards to remove expired cache records.
func (c *L1Cache) cleanup() {
	now := time.Now()
	for _, s := range c.shards {
		s.mu.Lock()
		for k, blockedUntil := range s.blocked {
			if now.After(blockedUntil) {
				delete(s.blocked, k)
			}
		}
		s.mu.Unlock()
	}
}

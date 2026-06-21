package limiter

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestL1Cache_Basic(t *testing.T) {
	cache := NewL1Cache(4)
	key := "user_1"

	// Verify key is initially not blocked
	if _, ok := cache.GetBlockedRemaining(key); ok {
		t.Fatalf("expected key %s not to be blocked initially", key)
	}

	// Block key for 100ms
	cache.Block(key, 100*time.Millisecond)

	// Check that key is now blocked
	remaining, ok := cache.GetBlockedRemaining(key)
	if !ok {
		t.Fatalf("expected key %s to be blocked", key)
	}
	if remaining <= 0 || remaining > 100*time.Millisecond {
		t.Fatalf("unexpected remaining time: %v", remaining)
	}

	// Wait for expiration
	time.Sleep(120 * time.Millisecond)

	// Verify key is no longer blocked (lazy eviction)
	if _, ok := cache.GetBlockedRemaining(key); ok {
		t.Fatalf("expected key %s block to have expired", key)
	}
}

func TestL1Cache_Cleanup(t *testing.T) {
	cache := NewL1Cache(4)
	cache.StartCleanup(10 * time.Millisecond)
	defer cache.Close()

	key1 := "user_1"
	key2 := "user_2"

	cache.Block(key1, 5*time.Millisecond)
	cache.Block(key2, 100*time.Millisecond)

	// Let key1 expire and cleanup task run
	time.Sleep(30 * time.Millisecond)

	// Verify key1 is cleaned from memory, but key2 remains
	cache.shards[fnv32(key1)&cache.shardMask].mu.RLock()
	_, exists1 := cache.shards[fnv32(key1)&cache.shardMask].blocked[key1]
	cache.shards[fnv32(key1)&cache.shardMask].mu.RUnlock()

	if exists1 {
		t.Fatalf("expected key1 to be cleaned up from the map entirely")
	}

	cache.shards[fnv32(key2)&cache.shardMask].mu.RLock()
	_, exists2 := cache.shards[fnv32(key2)&cache.shardMask].blocked[key2]
	cache.shards[fnv32(key2)&cache.shardMask].mu.RUnlock()

	if !exists2 {
		t.Fatalf("expected key2 to still exist in map")
	}
}

func TestL1Cache_Concurrency(t *testing.T) {
	cache := NewL1Cache(8)
	cache.StartCleanup(50 * time.Millisecond)
	defer cache.Close()

	var wg sync.WaitGroup
	workers := 20
	opsPerWorker := 500

	// Concurrent writes and reads
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("client_%d_%d", workerID, j)
				cache.Block(key, 10*time.Millisecond)
				cache.GetBlockedRemaining(key)
			}
		}(i)
	}

	wg.Wait()
}

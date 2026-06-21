package limiter

import "github.com/redis/go-redis/v9"

// slidingWindowScript is the Redis Lua script that implements the sliding window counter.
// It takes:
// - KEYS[1]: Current bucket key (e.g. "{user_123}:1718960000")
// - KEYS[2]: Previous bucket key (e.g. "{user_123}:1718959940")
// - ARGV[1]: Rate limit capacity (e.g. 100)
// - ARGV[2]: Window size in milliseconds (e.g. 60000)
// - ARGV[3]: Current timestamp in milliseconds
//
// It returns a list/slice:
// - [1] allowed (1 = true, 0 = false)
// - [2] remaining requests (integer)
// - [3] time until reset in milliseconds (integer)
var slidingWindowScript = redis.NewScript(`
local current_key = KEYS[1]
local previous_key = KEYS[2]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

local current_count = tonumber(redis.call('GET', current_key) or '0')
local previous_count = tonumber(redis.call('GET', previous_key) or '0')

-- Compute progress in current bucket and weight of previous bucket
local elapsed = now_ms % window_ms
local weight = (window_ms - elapsed) / window_ms
local estimated_count = math.floor(previous_count * weight + current_count)

if estimated_count < limit then
    local new_count = redis.call('INCR', current_key)
    if new_count == 1 then
        -- Set TTL to 2 * window to ensure current bucket exists long enough for the next window check
        redis.call('PEXPIRE', current_key, window_ms * 2)
    end
    local remaining = math.max(0, limit - (estimated_count + 1))
    local reset_ms = window_ms - elapsed
    return {1, remaining, reset_ms}
else
    local reset_ms = window_ms - elapsed
    return {0, 0, reset_ms}
end
`)

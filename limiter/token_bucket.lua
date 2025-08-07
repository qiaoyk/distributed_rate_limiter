-- KEYS[1]: The unique key for the rate limiter instance.
-- ARGV[1]: The rate of token generation per second.
-- ARGV[2]: The capacity of the token bucket.
-- ARGV[3]: The current time as a Unix timestamp (float).
-- ARGV[4]: The number of tokens to consume.

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local info = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(info[1])
local last_ts = tonumber(info[2])

-- Initialize if it's the first request.
if tokens == nil or last_ts == nil then
  tokens = capacity
  last_ts = now
end

-- Refill the bucket with new tokens based on elapsed time.
local elapsed = now - last_ts
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
end

local allowed = 0
-- If there are enough tokens, consume them and mark as allowed.
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

-- Update the token count and timestamp in Redis.
redis.call("HMSET", KEYS[1], "tokens", tokens, "ts", now)

-- Set a TTL on the key to prevent memory leaks from unused limiters.
-- A reasonable TTL is twice the time it takes to fill the bucket from empty, with a minimum.
local ttl = math.ceil((capacity / rate) * 2)
if ttl < 20 then
    ttl = 20
end
redis.call("EXPIRE", KEYS[1], ttl)

return allowed


-- KEYS[1]: the key for the token bucket (e.g., "rate_limiter:user123")
-- ARGV[1]: rate (tokens per second)
-- ARGV[2]: capacity (bucket size)
-- ARGV[3]: now (current timestamp in seconds with fractional part)
-- ARGV[4]: n (number of tokens to consume)
-- ARGV[5]: ttl (time-to-live in seconds for the key)

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local bucket = redis.call("hmget", key, "tokens", "last_op")
local tokens = tonumber(bucket[1])
local last_op = tonumber(bucket[2])

if tokens == nil or last_op == nil then
    tokens = capacity
    last_op = now
end

-- Refill tokens based on elapsed time
local elapsed = now - last_op
if elapsed > 0 then
    tokens = math.min(capacity, tokens + (rate * elapsed))
end
last_op = now

-- Calculate delay if needed
local delay = 0
if n > tokens then
    local needed = n - tokens
    delay = needed / rate
end

-- Always subtract tokens, creating a "debt" if necessary.
tokens = tokens - n

redis.call("hmset", key, "tokens", tokens, "last_op", last_op)
redis.call("expire", key, ttl)

-- Redis returns strings, so convert everything. The result is [allowed, tokens, retry_after]
-- In this implementation, we always "allow" the reservation, but might return a non-zero delay.
return {"1", tostring(tokens), tostring(delay)}

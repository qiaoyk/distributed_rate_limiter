-- KEYS[1]: the key for the token bucket, e.g., "limiter:{user_id}"
-- ARGV[1]: rate (tokens per second)
-- ARGV[2]: capacity (bucket size)
-- ARGV[3]: now (current timestamp in seconds with fractional part)
-- ARGV[4]: n (number of tokens to consume)
--
-- Returns a table with three values:
-- {
--   1: allowed (1 for yes, 0 for no),
--   2: tokens (the current number of tokens in the bucket),
--   3: retry_after (the recommended time in seconds to wait before retrying, -1 if allowed)
-- }

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested_tokens = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "tokens", "last_op")
local tokens = tonumber(state[1])
local last_op = tonumber(state[2])

if tokens == nil or last_op == nil then
  -- initialize the bucket.
  tokens = capacity
  last_op = now
end

-- Refill tokens based on elapsed time.
local elapsed = now - last_op
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
end

local allowed = 0
local retry_after = -1.0

if tokens >= requested_tokens then
  -- Enough tokens are available.
  tokens = tokens - requested_tokens
  allowed = 1
  redis.call("HMSET", key, "tokens", tokens, "last_op", now)
else
  -- Not enough tokens. Calculate how long to wait.
  local needed = requested_tokens - tokens
  retry_after = needed / rate
end

-- Convert numbers to strings to ensure consistent return types for parsing in Go.
return { tostring(allowed), tostring(tokens), tostring(retry_after) }


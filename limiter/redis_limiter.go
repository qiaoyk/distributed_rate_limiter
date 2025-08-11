package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/go-redis/redis/v8"
)

//go:embed token_bucket.lua
var luaScript string

// RedisLimiter is a distributed rate limiter based on the token bucket algorithm, using Redis and Lua script to ensure atomicity.
type RedisLimiter struct {
	client *redis.Client
	key    string  // This acts as a key prefix for namespacing.
	rate   float64 // tokens per second
	cap    float64 // bucket capacity
	script *redis.Script
	clock  Clock
}

// Option configures a RedisLimiter.
type Option func(*RedisLimiter)

// WithClock sets a custom clock for the limiter, useful for testing.
func WithClock(c Clock) Option {
	return func(l *RedisLimiter) {
		l.clock = c
	}
}

// NewRedisLimiter creates a new RedisLimiter.
// It loads the Lua script into Redis and stores the SHA hash for future calls.
func NewRedisLimiter(client *redis.Client, keyPrefix string, rate float64, capacity float64, opts ...Option) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}
	if keyPrefix == "" {
		return nil, errors.New("key prefix cannot be empty")
	}
	if capacity <= 0 || rate <= 0 {
		return nil, errors.New("capacity and rate must be greater than 0")
	}

	script := redis.NewScript(luaScript)
	// Pre-load the script on initialization for fail-fast behavior.
	if err := script.Load(context.Background(), client).Err(); err != nil {
		return nil, fmt.Errorf("failed to load lua script: %w", err)
	}

	limiter := &RedisLimiter{
		client: client,
		key:    keyPrefix,
		rate:   rate,
		cap:    capacity,
		script: script,
		clock:  NewRealClock(),
	}

	for _, opt := range opts {
		opt(limiter)
	}

	return limiter, nil
}

// Allow is a convenience method for AllowN(ctx, bizKey, 1).
func (l *RedisLimiter) Allow(ctx context.Context, bizKey string) (bool, error) {
	return l.AllowN(ctx, bizKey, 1)
}

// AllowN checks if n tokens can be taken from the bucket.
// The actual redis key will be a combination of the limiter's key prefix and the bizKey.
func (l *RedisLimiter) AllowN(ctx context.Context, bizKey string, n float64) (bool, error) {
	if bizKey == "" {
		return false, errors.New("bizKey cannot be fucking empty")
	}
	fullKey := fmt.Sprintf("%s:%s", l.key, bizKey)
	now := float64(l.clock.Now().UnixNano()) / 1e9

	result, err := l.script.Run(ctx, l.client, []string{fullKey}, l.rate, l.cap, now, n).Result()
	if err != nil {
		return false, fmt.Errorf("failed to execute lua script: %w", err)
	}

	allowed, ok := result.(int64)
	if !ok {
		return false, errors.New("unexpected result from redis script")
	}

	return allowed == 1, nil
}

package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/cast"
)

//go:embed token_bucket.lua
var luaScript string

// RedisLimiter is a distributed rate limiter based on the token bucket algorithm, using Redis and Lua script to ensure atomicity.
type RedisLimiter struct {
	client *redis.Client
	key    string // This acts as a key prefix for namespace.
	rate   float64
	cap    float64
	ttl    time.Duration
	script *redis.Script

	// For self-health-check
	isDown                 atomic.Bool
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	healthCheckInterval    time.Duration
	selfHealthCheckEnabled bool
}

// Option configures a RedisLimiter.
type Option func(*RedisLimiter)

// WithSelfHealthCheck enables a background health check goroutine for the RedisLimiter.
func WithSelfHealthCheck(interval time.Duration) Option {
	return func(l *RedisLimiter) {
		l.selfHealthCheckEnabled = true
		if interval > 0 {
			l.healthCheckInterval = interval
		} else {
			l.healthCheckInterval = 1 * time.Second // default
		}
	}
}

// NewRedisLimiter creates a new RedisLimiter.
func NewRedisLimiter(client *redis.Client, keyPrefix string, rate float64, capacity float64, ttl time.Duration, opts ...Option) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}
	if keyPrefix == "" {
		return nil, errors.New("key prefix cannot be empty")
	}
	if capacity <= 0 || rate <= 0 {
		return nil, errors.New("capacity and rate must be greater than 0")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be > 0")
	}

	// pre-load script
	script := redis.NewScript(luaScript)
	if err := script.Load(context.Background(), client).Err(); err != nil {
		return nil, fmt.Errorf("failed to load lua script: %w", err)
	}

	limiter := &RedisLimiter{
		client: client,
		key:    keyPrefix,
		rate:   rate,
		cap:    capacity,
		ttl:    ttl,
		script: script,
	}
	for _, opt := range opts {
		opt(limiter)
	}

	if limiter.selfHealthCheckEnabled {
		var ctx context.Context
		ctx, limiter.cancel = context.WithCancel(context.Background())
		limiter.wg.Add(1)
		go limiter.healthCheckLoop(ctx)
	}
	return limiter, nil
}

// Close stops the background health checker if it was started.
func (l *RedisLimiter) Close() {
	if l == nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
		l.wg.Wait()
	}
}

// Allow is a shortcut for AllowN(key, 1).
func (l *RedisLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *RedisLimiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if l.selfHealthCheckEnabled && l.isDown.Load() {
		return false, ErrPrimaryDown
	}
	res, err := l.ReserveN(ctx, key, n)
	if err != nil {
		return false, err
	}
	if !res.OK() {
		return false, nil // Not allowed, but no error
	}
	return res.Delay() == 0, nil
}

// Reserve is a shortcut for ReserveN(key, 1).
func (l *RedisLimiter) Reserve(ctx context.Context, key string) (*Reservation, error) {
	return l.ReserveN(ctx, key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
func (l *RedisLimiter) ReserveN(ctx context.Context, key string, n int) (*Reservation, error) {
	if l.selfHealthCheckEnabled && l.isDown.Load() {
		return nil, ErrPrimaryDown
	}
	if float64(n) > l.cap {
		return &Reservation{ok: false}, nil
	}

	res, err := l.reserveN(ctx, key, float64(n))
	if err != nil {
		return nil, err
	}

	delay := time.Duration(res.RetryAfter * float64(time.Second))
	now := time.Now()
	timeToAct := now.Add(delay)

	return &Reservation{
		ok:        true,
		timeToAct: timeToAct,
		key:       key,
		tokens:    n,
	}, nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *RedisLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *RedisLimiter) WaitN(ctx context.Context, key string, n int) error {
	if l.selfHealthCheckEnabled && l.isDown.Load() {
		return ErrPrimaryDown
	}
	res, err := l.ReserveN(ctx, key, n)
	if err != nil {
		return err
	}

	if !res.OK() {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's capacity %v", n, l.cap)
	}

	delay := res.Delay()
	if delay == 0 {
		return nil
	}

	if dl, ok := ctx.Deadline(); ok {
		if time.Now().Add(delay).After(dl) {
			return context.DeadlineExceeded
		}
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type allowResult struct {
	Allowed    bool
	Tokens     float64
	RetryAfter float64 // in seconds
}

func (l *RedisLimiter) reserveN(ctx context.Context, bizKey string, n float64) (*allowResult, error) {
	if bizKey == "" {
		return nil, errors.New("bizKey cannot be fucking empty")
	}
	fullKey := fmt.Sprintf("%s:%s", l.key, bizKey)
	now := float64(time.Now().UnixNano()) / 1e9

	res, err := l.script.Run(ctx, l.client, []string{fullKey}, l.rate, l.cap, now, n, l.ttl.Seconds()).Result()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: failed to execute lua script: %v", ErrPrimaryDown, err)
	}

	resSlice, ok := res.([]interface{})
	if !ok || len(resSlice) != 3 {
		return nil, errors.New("unexpected result from redis script")
	}

	allowedInt, err := cast.ToIntE(resSlice[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'allowed': %w", err)
	}
	tokens, err := cast.ToFloat64E(resSlice[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'tokens': %w", err)
	}
	retryAfter, err := cast.ToFloat64E(resSlice[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'retry_after': %w", err)
	}

	return &allowResult{
		Allowed:    allowedInt == 1,
		Tokens:     tokens,
		RetryAfter: retryAfter,
	}, nil
}

// HealthCheck checks the health of the limiter.
// It does so by trying to perform a zero-cost reservation on a test key.
func (l *RedisLimiter) HealthCheck(ctx context.Context) error {
	// Use reserveN with n=0 as the health check.
	_, err := l.reserveN(ctx, "health_check_key", 0)
	if err != nil {
		// The error from reserveN is already wrapped with ErrPrimaryDown
		return fmt.Errorf("redis health check failed: %w", err)
	}
	return nil
}

// healthCheckLoop is the internal loop for self-monitoring.
func (l *RedisLimiter) healthCheckLoop(ctx context.Context) {
	defer l.wg.Done()
	ticker := time.NewTicker(l.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Perform health check
			hctx, hcancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := l.HealthCheck(hctx)
			hcancel()

			if err != nil {
				// Health check failed, mark as down if not already.
				if !l.isDown.Load() {
					l.isDown.Store(true)
				}
			} else {
				// Health check succeeded, mark as up if it was down.
				if l.isDown.Load() {
					l.isDown.Store(false)
				}
			}
		}
	}
}

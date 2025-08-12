package limiter

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const numStripes = 256

// bucketState holds the state for a single token bucket.
type bucketState struct {
	tokens float64
	lastOp time.Time
}

// MemoryLimiter is a thread-safe, in-memory rate limiter. May be used for fallback logic of redis limiter.
type MemoryLimiter struct {
	locks   []sync.Mutex
	buckets *lru.Cache[string, *bucketState] // map[string]*bucketState
	rate    float64                          // rate of token generation per second
	cap     float64                          // capacity of the bucket
	clock   Clock
}

// NewMemoryLimiter creates a new MemoryLimiter.
func NewMemoryLimiter(rate float64, cap float64, opts ...MemoryOption) (*MemoryLimiter, error) {
	if rate <= 0 || cap <= 0 {
		return nil, errors.New("rate and cap must be greater than 0")
	}

	cache, err := lru.New[string, *bucketState](maxKeys)
	if err != nil {
		return nil, err
	}

	l := &MemoryLimiter{
		locks:   make([]sync.Mutex, numStripes),
		buckets: cache,
		rate:    rate,
		cap:     cap,
		clock:   NewRealClock(),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

func (l *MemoryLimiter) getLock(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &l.locks[h.Sum32()%numStripes]
}

// Allow is a shortcut for AllowN(key, 1).
func (l *MemoryLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *MemoryLimiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	res, err := l.ReserveN(ctx, key, n)
	if err != nil {
		return false, err
	}
	if !res.OK() {
		return false, nil
	}
	return res.Delay() == 0, nil
}

// Reserve is a shortcut for ReserveN(key, 1).
func (l *MemoryLimiter) Reserve(ctx context.Context, key string) (*Reservation, error) {
	return l.ReserveN(ctx, key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
func (l *MemoryLimiter) ReserveN(ctx context.Context, key string, n int) (*Reservation, error) {
	if float64(n) > l.cap {
		return &Reservation{ok: false}, nil
	}
	return l.reserveN(l.clock.Now(), key, n), nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *MemoryLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *MemoryLimiter) WaitN(ctx context.Context, key string, n int) error {
	r, err := l.ReserveN(ctx, key, n)
	if err != nil {
		return err
	}
	if !r.OK() {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's capacity %v", n, l.cap)
	}
	delay := r.Delay()
	if delay == 0 {
		return nil
	}

	if dl, ok := ctx.Deadline(); ok {
		if l.clock.Now().Add(delay).After(dl) {
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

// HealthCheck checks the health of the limiter.
func (l *MemoryLimiter) HealthCheck(_ context.Context) error {
	return nil // in-memory limiter is always healthy
}

func (l *MemoryLimiter) reserveN(now time.Time, key string, n int) *Reservation {
	mu := l.getLock(key)
	mu.Lock()
	defer mu.Unlock()

	state, ok := l.buckets.Get(key)
	if !ok {
		state = &bucketState{
			tokens: l.cap,
			lastOp: now,
		}
		l.buckets.Add(key, state)
	}

	// Refill tokens
	elapsed := now.Sub(state.lastOp)
	if elapsed > 0 {
		state.tokens += l.rate * elapsed.Seconds()
		if state.tokens > l.cap {
			state.tokens = l.cap
		}
	}
	state.lastOp = now

	tokensNeeded := float64(n)
	delay := time.Duration(0)
	if tokensNeeded > state.tokens {
		needed := tokensNeeded - state.tokens
		delay = time.Duration(needed/l.rate) * time.Second
	}

	// Always subtract tokens, creating a "debt" if necessary.
	state.tokens -= tokensNeeded

	return &Reservation{
		ok:        true,
		timeToAct: now.Add(delay),
		key:       key,
		tokens:    n,
	}
}

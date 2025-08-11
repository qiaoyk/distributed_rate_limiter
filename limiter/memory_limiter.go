package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// bucketState holds the state for a single token bucket.
type bucketState struct {
	tokens float64
	lastOp time.Time
}

// MemoryLimiter is a thread-safe, in-memory rate limiter. May be used for fallback logic of redis limiter.
type MemoryLimiter struct {
	lck     sync.Mutex
	buckets map[string]*bucketState
	rate    float64 // rate of token generation per second
	cap     float64 // capacity of the bucket
	clock   Clock
}

// NewMemoryLimiter creates a new MemoryLimiter.
func NewMemoryLimiter(rate float64, cap float64, opts ...Option) (*MemoryLimiter, error) {
	if rate <= 0 || cap <= 0 {
		return nil, errors.New("rate and cap must be greater than 0")
	}

	return &MemoryLimiter{
		buckets: make(map[string]*bucketState),
		rate:    rate,
		cap:     cap,
		clock:   NewRealClock(),
	}, nil
}

// Allow is a shortcut for AllowN(key, 1).
func (l *MemoryLimiter) Allow(key string) (bool, error) {
	return l.AllowN(key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *MemoryLimiter) AllowN(key string, n int) (bool, error) {
	res, err := l.ReserveN(key, n)
	if err != nil {
		return false, err
	}
	if !res.OK() {
		return false, nil
	}
	return res.Delay() == 0, nil
}

// Reserve is a shortcut for ReserveN(key, 1).
func (l *MemoryLimiter) Reserve(key string) (*Reservation, error) {
	return l.ReserveN(key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
func (l *MemoryLimiter) ReserveN(key string, n int) (*Reservation, error) {
	if float64(n) > l.cap {
		return &Reservation{ok: false, lim: l}, nil
	}
	return l.reserveN(l.clock.Now(), key, n), nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *MemoryLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *MemoryLimiter) WaitN(ctx context.Context, key string, n int) error {
	r, err := l.ReserveN(key, n)
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
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *MemoryLimiter) reserveN(now time.Time, key string, n int) *Reservation {
	l.lck.Lock()
	defer l.lck.Unlock()

	state, ok := l.buckets[key]
	if !ok {
		state = &bucketState{
			tokens: l.cap,
			lastOp: now,
		}
		l.buckets[key] = state
	}

	// Refill tokens
	elapsed := now.Sub(state.lastOp)
	if elapsed > 0 {
		state.tokens = math.Min(l.cap, state.tokens+(l.rate*elapsed.Seconds()))
	}
	state.lastOp = now

	tokensNeeded := float64(n)
	delay := time.Duration(0)
	if tokensNeeded > state.tokens {
		needed := tokensNeeded - state.tokens
		delay = time.Duration(needed/l.rate) * time.Second
	}

	if delay == 0 {
		state.tokens -= tokensNeeded
	}

	return &Reservation{
		ok:        true,
		timeToAct: now.Add(delay),
		lim:       l,
		key:       key,
		tokens:    n,
	}
}

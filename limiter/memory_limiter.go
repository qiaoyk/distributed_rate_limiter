package limiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// bucketState holds the state for a single token bucket.
type bucketState struct {
	tokens float64
	lastOp time.Time
}

// MemoryLimiter is a thread-safe, in-memory rate limiter that can manage multiple buckets
// distinguished by a business key (bizKey).
type MemoryLimiter struct {
	lck     sync.Mutex
	buckets map[string]*bucketState
	rate    float64 // rate of token generation per second
	cap     float64 // capacity of the bucket
}

// NewMemoryLimiter creates a new MemoryLimiter.
func NewMemoryLimiter(rate float64, cap float64) (*MemoryLimiter, error) {
	if rate <= 0 || cap <= 0 {
		return nil, errors.New("rate and cap must be greater than 0")
	}

	return &MemoryLimiter{
		buckets: make(map[string]*bucketState),
		rate:    rate,
		cap:     cap,
	}, nil
}

// Allow is a convenience method for AllowN(ctx, bizKey, 1).
func (l *MemoryLimiter) Allow(ctx context.Context, bizKey string) (bool, error) {
	return l.AllowN(ctx, bizKey, 1)
}

// AllowN checks if n tokens can be taken from the bucket for a given business key.
func (l *MemoryLimiter) AllowN(ctx context.Context, bizKey string, n float64) (bool, error) {
	if bizKey == "" {
		return false, errors.New("bizKey cannot be fucking empty")
	}

	l.lck.Lock()
	defer l.lck.Unlock()

	// Get or create the bucket for the specified bizKey.
	state, ok := l.buckets[bizKey]
	if !ok {
		state = &bucketState{
			tokens: l.cap,
			lastOp: time.Now(),
		}
		l.buckets[bizKey] = state
	}

	// Refill the bucket with new tokens based on the elapsed time.
	now := time.Now()
	elapsed := now.Sub(state.lastOp)
	if elapsed > 0 {
		state.tokens = math.Min(l.cap, state.tokens+(l.rate*elapsed.Seconds()))
		state.lastOp = now
	}

	if state.tokens >= n {
		state.tokens -= n
		return true, nil
	}

	return false, nil
}

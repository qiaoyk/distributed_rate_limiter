package limiter

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/time/rate"
)

const numStripes = 256

// StdMemoryLimiter is a thread-safe, in-memory rate limiter that uses the standard library's rate limiter.
type StdMemoryLimiter struct {
	locks   []sync.Mutex
	buckets *lru.Cache[string, *rate.Limiter]
	rate    rate.Limit // rate of token generation per second
	cap     int        // capacity of the bucket
}

// NewStdMemoryLimiter creates a new StdMemoryLimiter.
func NewStdMemoryLimiter(r float64, b int) (*StdMemoryLimiter, error) {
	if r <= 0 || b <= 0 {
		return nil, errors.New("rate and cap must be greater than 0")
	}

	cache, err := lru.New[string, *rate.Limiter](maxKeys)
	if err != nil {
		return nil, err
	}

	return &StdMemoryLimiter{
		locks:   make([]sync.Mutex, numStripes),
		buckets: cache,
		rate:    rate.Limit(r),
		cap:     b,
	}, nil
}

func (l *StdMemoryLimiter) getLock(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &l.locks[h.Sum32()%numStripes]
}

// Allow is a shortcut for AllowN(key, 1).
func (l *StdMemoryLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *StdMemoryLimiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	return l.getLimiter(key).AllowN(time.Now(), n), nil
}

// Reserve is a shortcut for ReserveN(key, 1).
func (l *StdMemoryLimiter) Reserve(ctx context.Context, key string) (*Reservation, error) {
	return l.ReserveN(ctx, key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
func (l *StdMemoryLimiter) ReserveN(ctx context.Context, key string, n int) (*Reservation, error) {
	res := l.getLimiter(key).ReserveN(time.Now(), n)
	if !res.OK() {
		// This indicates that the request is impossible to satisfy, for example, if n > burst size.
		return &Reservation{ok: false}, nil
	}
	return &Reservation{
		ok:        true,
		timeToAct: time.Now().Add(res.Delay()),
		key:       key,
		tokens:    n,
	}, nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *StdMemoryLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *StdMemoryLimiter) WaitN(ctx context.Context, key string, n int) error {
	lim := l.getLimiter(key)
	return lim.WaitN(ctx, n)
}

// HealthCheck checks the health of the limiter.
func (l *StdMemoryLimiter) HealthCheck(_ context.Context) error {
	return nil // in-memory limiter is always healthy
}

func (l *StdMemoryLimiter) getLimiter(key string) *rate.Limiter {
	mu := l.getLock(key)
	mu.Lock()
	defer mu.Unlock()

	lim, ok := l.buckets.Get(key)
	if !ok {
		lim = rate.NewLimiter(l.rate, l.cap)
		l.buckets.Add(key, lim)
	}
	return lim
}

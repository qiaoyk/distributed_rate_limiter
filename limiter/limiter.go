// Package limiter provides a rate limiter interface and implementations. Logic is based on "golang.org/x/time/rate".
package limiter

import (
	"context"
	"errors"
	"math"
	"time"
)

var ErrPrimaryDown = errors.New("limiter: primary component is unavailable")

const Inf = time.Duration(math.MaxInt64)

const maxKeys = 10000

// MemoryOption configures a MemoryLimiter.
type MemoryOption func(*MemoryLimiter)

// WithMemoryClock sets a custom clock for the memory limiter, for testing.
func WithMemoryClock(c Clock) MemoryOption {
	return func(l *MemoryLimiter) {
		l.clock = c
	}
}

// FallbackOption configures a FallbackLimiter.
type FallbackOption func(*FallbackLimiter)

// WithHealthCheckInterval sets the interval for health checks.
func WithHealthCheckInterval(d time.Duration) FallbackOption {
	return func(l *FallbackLimiter) {
		l.healthCheckInterval = d
	}
}

type Limiter interface {
	Wait(ctx context.Context, key string) error
	WaitN(ctx context.Context, key string, n int) error
	Allow(ctx context.Context, key string) (bool, error)
	AllowN(ctx context.Context, key string, n int) (bool, error)
	Reserve(ctx context.Context, key string) (*Reservation, error)
	ReserveN(ctx context.Context, key string, n int) (*Reservation, error)
	HealthCheck(ctx context.Context) error
}

type Reservation struct {
	ok        bool
	timeToAct time.Time
	key       string
	tokens    int
}

// OK returns whether the limiter can provide the requested number of tokens
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is the amount of time that the caller must wait before the events can happen.
func (r *Reservation) Delay() time.Duration {
	if !r.ok {
		return Inf
	}
	delay := time.Until(r.timeToAct)
	if delay < 0 {
		return 0
	}
	return delay
}

// Every returns a Limiter that allows events up to rate r and permits bursts of at most b tokens.
func Every(rate float64, b int) (Limiter, error) {
	return NewMemoryLimiter(rate, float64(b))
}

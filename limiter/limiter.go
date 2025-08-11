// Package limiter provides a rate limiter interface and implementations. Logic is based on "golang.org/x/time/rate".
package limiter

import (
	"context"
	"math"
	"time"
)

const Inf = time.Duration(math.MaxInt64)

type Limiter interface {
	Wait(ctx context.Context, key string) error
	WaitN(ctx context.Context, key string, n int) error
	Allow(key string) (bool, error)
	AllowN(key string, n int) (bool, error)
	Reserve(key string) (*Reservation, error)
	ReserveN(key string, n int) (*Reservation, error)
}

type Reservation struct {
	ok        bool
	lim       Limiter
	key       string
	tokens    int
	timeToAct time.Time
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

// Cancel indicates that the reservation is no longer needed.
func (r *Reservation) Cancel() {
	if !r.ok {
		return
	}
	r.ok = false
}

// Every returns a Limiter that allows events up to rate r and permits bursts of at most b tokens.
func Every(rate float64, b int) (Limiter, error) {
	return NewMemoryLimiter(rate, float64(b))
}

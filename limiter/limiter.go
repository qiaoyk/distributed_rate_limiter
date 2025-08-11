package limiter

import (
	"context"
	"math"
	"time"
)

// Inf is the infinite rate limit; it allows all events.
const Inf = time.Duration(math.MaxInt64)

// A Limiter controls how frequently events are allowed to happen.
type Limiter interface {
	Wait(ctx context.Context, key string) error
	WaitN(ctx context.Context, key string, n int) error
	Allow(key string) (bool, error)
	AllowN(key string, n int) (bool, error)
	Reserve(key string) (*Reservation, error)
	ReserveN(key string, n int) (*Reservation, error)
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok        bool
	lim       Limiter
	key       string
	tokens    int
	timeToAct time.Time
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is the amount of time that the caller must wait before the events can happen.
func (r *Reservation) Delay() time.Duration {
	if !r.ok {
		return Inf
	}
	delay := r.timeToAct.Sub(time.Now())
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
	// This part is complex to implement correctly for distributed limiters.
	// For now, we are not implementing the token return mechanism.
	// In a real-world scenario, this might involve another script call to Redis.
	r.ok = false
}

// Every returns a Limiter that allows events up to rate r and permits bursts of at most b tokens.
func Every(rate float64, b int) (Limiter, error) {
	return NewMemoryLimiter(rate, float64(b))
}

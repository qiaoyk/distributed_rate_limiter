package limiter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// FallbackLimiter provides a fallback mechanism. It uses a primary limiter and falls back to a secondary one if the primary fails.
type FallbackLimiter struct {
	primary             Limiter
	secondary           Limiter
	isPrimaryDown       atomic.Bool
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	healthCheckInterval time.Duration
}

// NewFallbackLimiter creates a new FallbackLimiter.
func NewFallbackLimiter(primary, secondary Limiter, opts ...FallbackOption) (*FallbackLimiter, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary limiter can't be nil")
	}
	if secondary == nil {
		return nil, fmt.Errorf("secondary limiter can't be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	fl := &FallbackLimiter{
		primary:             primary,
		secondary:           secondary,
		cancel:              cancel,
		healthCheckInterval: 1 * time.Second, // default interval
	}

	for _, opt := range opts {
		fl.apply(opt)
	}

	fl.wg.Add(1)
	go fl.healthCheckLoop(ctx)
	return fl, nil
}

func (l *FallbackLimiter) apply(opt FallbackOption) {
	opt(l)
}

// Allow reports whether the event may happen now.
func (l *FallbackLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *FallbackLimiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if l.isPrimaryDown.Load() {
		return l.secondary.AllowN(ctx, key, n)
	}

	allowed, err := l.primary.AllowN(ctx, key, n)
	if err != nil {
		if errors.Is(err, ErrPrimaryDown) {
			log.Printf("primary limiter failed, falling back to secondary: %v", err)
			l.isPrimaryDown.Store(true)
			return l.secondary.AllowN(ctx, key, n)
		}
		return false, err // non-fallback error
	}
	return allowed, nil
}

// Reserve returns a Reservation that indicates how long the caller must wait before the next event can happen.
func (l *FallbackLimiter) Reserve(ctx context.Context, key string) (*Reservation, error) {
	return l.ReserveN(ctx, key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events can happen.
func (l *FallbackLimiter) ReserveN(ctx context.Context, key string, n int) (*Reservation, error) {
	if l.isPrimaryDown.Load() {
		return l.secondary.ReserveN(ctx, key, n)
	}

	r, err := l.primary.ReserveN(ctx, key, n)
	if err != nil {
		if errors.Is(err, ErrPrimaryDown) {
			log.Printf("primary limiter reservation failed, falling back to secondary: %v", err)
			l.isPrimaryDown.Store(true)
			return l.secondary.ReserveN(ctx, key, n)
		}
		return nil, err // non-fallback error
	}
	return r, nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *FallbackLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *FallbackLimiter) WaitN(ctx context.Context, key string, n int) error {
	if l.isPrimaryDown.Load() {
		return l.secondary.WaitN(ctx, key, n)
	}

	err := l.primary.WaitN(ctx, key, n)
	if err != nil {
		if errors.Is(err, ErrPrimaryDown) {
			log.Printf("primary limiter failed on WaitN, falling back to secondary: %v", err)
			l.isPrimaryDown.Store(true)
			return l.secondary.WaitN(ctx, key, n)
		}
		return err // non-fallback error
	}
	return nil
}

// healthCheckLoop periodically checks if the primary limiter has recovered.
func (l *FallbackLimiter) healthCheckLoop(ctx context.Context) {
	defer l.wg.Done()
	ticker := time.NewTicker(l.healthCheckInterval)
	defer ticker.Stop()

	log.Println("Health checker started. Will check primary status periodically.")

	for {
		select {
		case <-ctx.Done():
			log.Println("Health checker shutting down.")
			return
		case <-ticker.C:
			subCtx, subCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := l.primary.HealthCheck(subCtx)
			subCancel()
			if err != nil {
				if !l.isPrimaryDown.Load() {
					log.Println("Primary limiter health check failed. Switching to secondary.")
					l.isPrimaryDown.Store(true)
				}
			} else {
				if l.isPrimaryDown.Load() {
					log.Println("Primary limiter is healthy now.")
					l.isPrimaryDown.Store(false)
				}
			}
		}
	}
}

// Close stops the health check goroutine. Plz call this method to avoid leaking goroutines.
func (l *FallbackLimiter) Close() {
	if l == nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
}

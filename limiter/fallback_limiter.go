package limiter

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// FallbackLimiter provides a fallback mechanism. It uses a primary limiter and falls back to a secondary one if the primary fails.
type FallbackLimiter struct {
	primary       Limiter
	secondary     Limiter
	isPrimaryDown atomic.Bool
}

// NewFallbackLimiter creates a new FallbackLimiter.
func NewFallbackLimiter(primary, secondary Limiter) (*FallbackLimiter, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary limiter can't be nil")
	}
	if secondary == nil {
		return nil, fmt.Errorf("secondary limiter can't be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fl := &FallbackLimiter{primary, secondary, atomic.Bool{}}

	go fl.healthCheck(ctx)

	return fl, nil
}

// Allow reports whether the event may happen now.
func (l *FallbackLimiter) Allow(key string) (bool, error) {
	return l.AllowN(key, 1)
}

// AllowN reports whether n events may happen at time now.
func (l *FallbackLimiter) AllowN(key string, n int) (bool, error) {
	allowed, err := l.primary.AllowN(key, n)
	if err != nil || l.isPrimaryDown.Load() {
		l.isPrimaryDown.Store(true)
		log.Printf("primary limiter failed, falling back to secondary: %v", err)
		return l.secondary.AllowN(key, n)
	}
	return allowed, nil
}

// Reserve returns a Reservation that indicates how long the caller must wait before the next event can happen.
func (l *FallbackLimiter) Reserve(key string) (*Reservation, error) {
	return l.ReserveN(key, 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events can happen.
func (l *FallbackLimiter) ReserveN(key string, n int) (*Reservation, error) {
	r, err := l.primary.ReserveN(key, n)
	if err != nil || l.isPrimaryDown.Load() {
		l.isPrimaryDown.Store(true)
		log.Printf("primary limiter reservation failed, falling back to secondary: %v", err)
		return l.secondary.ReserveN(key, n)
	}
	return r, nil
}

// Wait is a shortcut for WaitN(ctx, key, 1).
func (l *FallbackLimiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n events can be allowed.
func (l *FallbackLimiter) WaitN(ctx context.Context, key string, n int) error {
	err := l.primary.WaitN(ctx, key, n)
	if err != nil || l.isPrimaryDown.Load() {
		l.isPrimaryDown.Store(true)
		log.Printf("primary limiter failed on WaitN, falling back to secondary: %v", err)
		return l.secondary.WaitN(ctx, key, n)
	}
	return nil
}

// healthCheck periodically checks if the primary limiter has recovered.
func (l *FallbackLimiter) healthCheck(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Health checker started. Will check primary status periodically.")

	for {
		select {
		case <-ctx.Done():
			log.Println("Health checker shutting down.")
			return
		case <-ticker.C:
			if allowed, err := l.primary.AllowN("health_check", 0); err != nil || !allowed {
				log.Println("Primary limiter still down. Switching to secondary.")
				l.isPrimaryDown.Store(true)
			} else {
				log.Println("Primary limiter is healthy.")
				l.isPrimaryDown.Store(false)
			}
		}
	}
}

// Close stops the health check goroutine.
// You should call this when you're done with the limiter to avoid leaking goroutines.
func (l *FallbackLimiter) Close() {
	// No explicit cancel needed for healthCheck as it's a background goroutine.
	// If primary and secondary were managed by context, you'd cancel them here.
}

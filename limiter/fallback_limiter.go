package limiter

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// Limiter is the interface that all limiters should implement.
// This makes them interchangeable, which is the whole fucking point.
type Limiter interface {
	Allow(ctx context.Context, bizKey string) (bool, error)
	AllowN(ctx context.Context, bizKey string, n float64) (bool, error)
}

// FallbackLimiter provides a fallback mechanism. It uses the primary limiter by default
// and switches to the fallback limiter if the primary is down.
type FallbackLimiter struct {
	primary       Limiter
	fallback      Limiter
	isPrimaryDown atomic.Bool
	cancel        context.CancelFunc
}

// NewFallbackLimiter creates a new FallbackLimiter.
// It also starts a background goroutine to check the health of the primary limiter.
func NewFallbackLimiter(primary Limiter, fallback Limiter) (*FallbackLimiter, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary limiter can't be nil")
	}
	if fallback == nil {
		return nil, fmt.Errorf("fallback limiter can't be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	fl := &FallbackLimiter{
		primary:  primary,
		fallback: fallback,
		cancel:   cancel,
	}
	fl.isPrimaryDown.Store(false)

	go fl.healthCheck(ctx)

	return fl, nil
}

// AllowN checks if n tokens can be taken. It attempts to use the primary limiter first.
// If the primary fails, it transparently switches to the fallback limiter.
func (l *FallbackLimiter) AllowN(ctx context.Context, bizKey string, n float64) (bool, error) {
	if l.isPrimaryDown.Load() {
		return l.useFallback(ctx, bizKey, n)
	}

	allowed, err := l.primary.AllowN(ctx, bizKey, n)
	if err != nil {
		log.Printf("FUCK! Primary limiter failed: %v. Switching to fallback.", err)
		l.isPrimaryDown.Store(true)
		return l.useFallback(ctx, bizKey, n)
	}

	return allowed, nil
}

// Allow is a convenience method for AllowN(ctx, bizKey, 1).
func (l *FallbackLimiter) Allow(ctx context.Context, bizKey string) (bool, error) {
	return l.AllowN(ctx, bizKey, 1)
}

// useFallback is a helper to encapsulate the fallback logic.
func (l *FallbackLimiter) useFallback(ctx context.Context, bizKey string, n float64) (bool, error) {
	log.Println("Using fallback limiter.")
	allowed, err := l.fallback.AllowN(ctx, bizKey, n)
	if err != nil {
		// If the fallback also fails, we're fucked.
		log.Printf("SHIT! Fallback limiter also failed: %v", err)
		return false, err
	}
	return allowed, nil
}

// healthCheck periodically checks if the primary limiter has recovered.
func (l *FallbackLimiter) healthCheck(ctx context.Context) {
	// Don't spam the health check. Once every few seconds is enough.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Health checker started. Will check primary status periodically.")

	for {
		select {
		case <-ctx.Done():
			log.Println("Health checker shutting down.")
			return
		case <-ticker.C:
			// Only check if we are currently in fallback mode.
			if l.isPrimaryDown.Load() {
				// Use a short timeout for the health check to avoid blocking for too long.
				checkCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				// We use AllowN with 0 tokens to just check the connection without consuming tokens.
				if _, err := l.primary.AllowN(checkCtx, "health_check", 0); err == nil {
					log.Println("GOOD NEWS! Primary limiter has recovered. Switching back.")
					l.isPrimaryDown.Store(false)
				} else {
					log.Printf("Primary limiter still down: %v", err)
				}
				cancel()
			}
		}
	}
}

// Close stops the health check goroutine.
// You should call this when you're done with the limiter to avoid leaking goroutines.
func (l *FallbackLimiter) Close() {
	l.cancel()
}

package limiter

import "time"

// Clock abstracts time-related functions for testability.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (c *realClock) Now() time.Time {
	return time.Now()
}

// NewRealClock returns a clock that uses the system's time.
func NewRealClock() Clock {
	return &realClock{}
}

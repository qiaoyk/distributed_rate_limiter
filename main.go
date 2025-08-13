package main

import (
	"rate_limiter/mock"
)

func main() {
	mock.FallbackLimiterMultiInstanceMock(true)
}

package main

import (
	"rate_limiter/mock"
)

func main() {
	mock.FallbackLimiterMock(true)
}

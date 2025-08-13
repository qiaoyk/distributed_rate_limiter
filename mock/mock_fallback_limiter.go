package mock

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"rate_limiter/limiter"

	"github.com/go-redis/redis/v8"
)

func FallbackLimiterMock(needMockRedisDown bool) {
	const (
		redisAddr           = "localhost:6379"
		workerCount         = 5
		requestsPerWorker   = 50
		logChanSize         = workerCount * requestsPerWorker
		primaryRate         = 10.0
		primaryCapacity     = 10.0
		primaryTTL          = 10 * time.Second
		secondaryRate       = 2.0
		secondaryCapacity   = 2.0
		healthCheckInterval = 500 * time.Millisecond
	)

	totalRequests := workerCount * requestsPerWorker

	// Setup logging
	logChan := make(chan string, logChanSize)
	var logWg sync.WaitGroup
	logWg.Add(1)
	go func() {
		defer logWg.Done()
		f, err := os.OpenFile("mock.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open mock.log: %v", err)
		}
		defer f.Close()

		for msg := range logChan {
			if _, err := fmt.Fprintln(f, msg); err != nil {
				log.Printf("Failed to write to mock.log: %v", err)
			}
		}
	}()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("Redis client close error: %v", err)
		}
	}()

	if needMockRedisDown {
		go func() {
			time.Sleep(2 * time.Second)
			log.Println("--- MOCKING REDIS DOWN ---")
			if err := rdb.Close(); err != nil {
				log.Printf("Failed to manually close Redis client: %v", err)
			} else {
				log.Println("--- REDIS CONNECTION MANUALLY CLOSED ---")
			}
		}()
	}

	// Create shared limiters
	rl, err := limiter.NewRedisLimiter(rdb, "demo:limiter", primaryRate, primaryCapacity, primaryTTL)
	if err != nil {
		log.Fatalf("Failed to create redis limiter: %v", err)
	}

	ml, err := limiter.NewStdMemoryLimiter(secondaryRate, secondaryCapacity)
	if err != nil {
		log.Fatalf("Failed to create memory limiter: %v", err)
	}

	fl, err := limiter.NewFallbackLimiter(rl, ml, limiter.WithHealthCheckInterval(healthCheckInterval))
	if err != nil {
		log.Fatalf("Failed to create fallback limiter: %v", err)
	}
	defer fl.Close()

	var wg sync.WaitGroup
	var successfulRequests atomic.Int64
	st := time.Now()

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < requestsPerWorker; i++ {
				startWait := time.Now()
				err := fl.Wait(context.Background(), "user:wait_test")
				waitTime := time.Since(startWait)

				ts := time.Now().Format("2006-01-02 15:04:05.000")
				if err != nil {
					logChan <- fmt.Sprintf("[%s] Worker %d request %d FAILED: %v", ts, workerID, i+1, err)
				} else {
					successfulRequests.Add(1)
					logChan <- fmt.Sprintf("[%s] Worker %d request %d processed, waited: %v", ts, workerID, i+1, waitTime)
				}
			}
		}(w + 1)
	}

	wg.Wait()
	close(logChan) // Close channel to signal logger to exit
	logWg.Wait()   // Wait for logger to finish writing

	duration := time.Since(st)
	successCount := successfulRequests.Load()
	log.Printf("Finished %d total requests in %.2f seconds.", totalRequests, duration.Seconds())
	log.Printf("Successful: %d, Failed: %d", successCount, totalRequests-int(successCount))
	log.Printf("Observed QPS (successful requests): %.2f", float64(successCount)/duration.Seconds())
}

func FallbackLimiterMultiInstanceMock(needMockRedisDown bool) {
	const (
		redisAddr           = "localhost:6379"
		workerCount         = 5
		requestsPerWorker   = 50
		logChanSize         = workerCount * requestsPerWorker
		primaryRate         = 10.0
		primaryCapacity     = 10.0
		primaryTTL          = 10 * time.Second
		secondaryRate       = 2.0
		secondaryCapacity   = 2.0
		healthCheckInterval = 500 * time.Millisecond
	)

	totalRequests := workerCount * requestsPerWorker

	// Setup logging
	logChan := make(chan string, logChanSize)
	var logWg sync.WaitGroup
	logWg.Add(1)
	go func() {
		defer logWg.Done()
		f, err := os.OpenFile("mock.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open mock.log: %v", err)
		}
		defer f.Close()

		for msg := range logChan {
			if _, err := fmt.Fprintln(f, msg); err != nil {
				log.Printf("Failed to write to mock.log: %v", err)
			}
		}
	}()

	// Setup a single shared Redis client for all instances
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	if needMockRedisDown {
		go func() {
			time.Sleep(2 * time.Second)
			log.Println("--- MOCKING REDIS DOWN ---")
			if err := rdb.Close(); err != nil {
				// Don't log error if it's already closed, which is expected
			} else {
				log.Println("--- REDIS CONNECTION MANUALLY CLOSED ---")
			}
		}()
	}

	var wg sync.WaitGroup
	var successfulRequests atomic.Int64
	st := time.Now()

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each goroutine (service instance) creates its own limiter instance.
			// They share the Redis state via the common rdb client.
			rl, err := limiter.NewRedisLimiter(rdb, "demo:limiter", primaryRate, primaryCapacity, primaryTTL)
			if err != nil {
				log.Printf("[Worker %d] Failed to create redis limiter: %v", workerID, err)
				return
			}
			ml, err := limiter.NewStdMemoryLimiter(secondaryRate, secondaryCapacity)
			if err != nil {
				log.Printf("[Worker %d] Failed to create memory limiter: %v", workerID, err)
				return
			}
			fl, err := limiter.NewFallbackLimiter(rl, ml, limiter.WithHealthCheckInterval(healthCheckInterval))
			if err != nil {
				log.Printf("[Worker %d] Failed to create fallback limiter: %v", workerID, err)
				return
			}
			defer fl.Close()

			for i := 0; i < requestsPerWorker; i++ {
				startWait := time.Now()
				err := fl.Wait(context.Background(), "user:wait_test")
				waitTime := time.Since(startWait)

				ts := time.Now().Format("2006-01-02 15:04:05.000")
				if err != nil {
					logChan <- fmt.Sprintf("[%s] Worker %d request %d FAILED: %v", ts, workerID, i+1, err)
				} else {
					successfulRequests.Add(1)
					logChan <- fmt.Sprintf("[%s] Worker %d request %d processed, waited: %v", ts, workerID, i+1, waitTime)
				}
			}
		}(w + 1)
	}

	wg.Wait()
	close(logChan)
	logWg.Wait()

	duration := time.Since(st)
	successCount := successfulRequests.Load()
	log.Printf("Finished %d total requests in %.2f seconds.", totalRequests, duration.Seconds())
	log.Printf("Successful: %d, Failed: %d", successCount, totalRequests-int(successCount))
	log.Printf("Observed QPS (successful requests): %.2f", float64(successCount)/duration.Seconds())
}

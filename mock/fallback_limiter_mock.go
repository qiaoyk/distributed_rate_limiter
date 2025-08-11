package mock

import (
	"context"
	"log"
	"sync"
	"time"

	"rate_limiter/limiter"

	"github.com/go-redis/redis/v8"
)

func FallbackLimiterMock(needMockRedisDown bool) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("Redis client close error: %v", err)
		}
	}()

	if needMockRedisDown {
		go func() {
			time.Sleep(2 * time.Second)
			log.Println("mock redis down")
			if err := rdb.Close(); err != nil {
				log.Printf("Redis client close error: %v", err)
			} else {
				log.Println("Redis连接已手动关闭")
			}
		}()
	}

	rl, err := limiter.NewRedisLimiter(rdb, "demo:limiter", 10, 10)
	if err != nil {
		log.Fatalf("限流器初始化失败 err: %v", err)
	}

	ml, err := limiter.NewMemoryLimiter(2, 2)
	if err != nil {
		log.Fatalf("限流器初始化失败 err: %v", err)
	}

	fl, err := limiter.NewFallbackLimiter(rl, ml)
	if err != nil {
		log.Fatalf("限流器初始化失败 err: %v", err)
	}

	var wg sync.WaitGroup
	workerCount := 5
	requestsPerWorker := 50
	totalRequests := workerCount * requestsPerWorker

	st := time.Now()
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < requestsPerWorker; i++ {
				err := fl.Wait(context.Background(), "user:wait_test")
				if err != nil {
					log.Printf("[Worker %d] Wait returned an error: %v", workerID, err)
				} else {
					log.Printf("[Worker %d] Request %d proceeded after waiting", workerID, i+1)
				}
				// Add a small delay to simulate work being done.
				time.Sleep(50 * time.Millisecond)
			}
		}(w + 1)
	}

	wg.Wait()

	duration := time.Since(st)
	log.Printf("Finished %d requests in %.2f seconds.", totalRequests, duration.Seconds())
	log.Printf("Observed QPS: %.2f", float64(totalRequests)/duration.Seconds())
}

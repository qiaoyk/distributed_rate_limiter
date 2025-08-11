package mock

import (
	"context"
	"fmt"
	"log"
	"os"
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

	var wg sync.WaitGroup
	workerCount := 5
	requestsPerWorker := 50
	totalRequests := workerCount * requestsPerWorker

	st := time.Now()
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each goroutine creates its own limiter instance, simulating separate services.
			rl, err := limiter.NewRedisLimiter(rdb, "demo:limiter", 10, 10)
			if err != nil {
				log.Printf("[Worker %d] Failed to create redis limiter: %v", workerID, err)
				return
			}

			ml, err := limiter.NewMemoryLimiter(2, 2)
			if err != nil {
				log.Printf("[Worker %d] Failed to create memory limiter: %v", workerID, err)
				return
			}

			fl, err := limiter.NewFallbackLimiter(rl, ml)
			if err != nil {
				log.Printf("[Worker %d] Failed to create fallback limiter: %v", workerID, err)
				return
			}

			for i := 0; i < requestsPerWorker; i++ {
				err := fl.Wait(context.Background(), "user:wait_test")
				if err != nil {
					log.Printf("[Worker %d] Wait returned an error: %v", workerID, err)
				} else {
					// 模拟业务处理：向 mock.log 写一行
					f, ferr := os.OpenFile("mock.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
					if ferr != nil {
						log.Printf("[Worker %d] Failed to open mock.log: %v", workerID, ferr)
					} else {
						ts := time.Now().Format("2006-01-02 15:04:05.000")
						_, _ = fmt.Fprintf(f, "[%s] Worker %d processed request %d\n", ts, workerID, i+1)
						_ = f.Close()
					}
				}
			}
		}(w + 1)
	}

	wg.Wait()

	duration := time.Since(st)
	log.Printf("Finished %d requests in %.2f seconds.", totalRequests, duration.Seconds())
	log.Printf("Observed QPS: %.2f", float64(totalRequests)/duration.Seconds())
}

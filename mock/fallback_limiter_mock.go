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
		log.Fatalf("限流器都初始化失败 err: %v", err)
	}

	ml, err := limiter.NewMemoryLimiter(2, 2)
	if err != nil {
		log.Fatalf("限流器都初始化失败 err: %v", err)
	}

	fl, err := limiter.NewFallbackLimiter(rl, ml)
	if err != nil {
		log.Fatalf("限流器都初始化失败 err: %v", err)
	}

	var wg sync.WaitGroup
	workerCount := 5
	requestsPerWorker := 500
	totalRequests := workerCount * requestsPerWorker

	passCh := make(chan int, workerCount)
	blockCh := make(chan int, workerCount)

	ctx := context.Background()

	st := time.Now()
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(time.Second / 20) // 每个协程20 QPS，合起来直接爆表
			defer ticker.Stop()
			pass, block := 0, 0
			for i := 0; i < requestsPerWorker; i++ {
				<-ticker.C
				allowed, err := fl.Allow(ctx, "user:42")
				if err != nil {
					log.Printf("[工人%d] 限流器报错了: %v", workerID, err)
					continue
				}
				if allowed {
					pass++
					log.Printf("[工人%d] 第%d次请求：放行", workerID, i+1)
				} else {
					block++
					// log.Printf("[工人%d] 第%d次请求：被限流", workerID, i+1)
				}
			}
			passCh <- pass
			blockCh <- block
		}(w + 1)
	}

	wg.Wait()
	close(passCh)
	close(blockCh)

	totalPass, totalBlock := 0, 0
	for p := range passCh {
		totalPass += p
	}
	for b := range blockCh {
		totalBlock += b
	}
	log.Printf("多协程测试结束！总请求数：%d，放行：%d，限流：%d， 平均QPS：%.2f", totalRequests, totalPass, totalBlock, float64(totalPass)/time.Since(st).Seconds())
}

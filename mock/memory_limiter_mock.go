package mock

import (
	"context"
	"log"
	"sync"
	"time"

	"rate_limiter/limiter"
)

func MemLimiterMock() {

	ml, err := limiter.NewMemoryLimiter(10, 10)
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
				allowed, err := ml.Allow(ctx, "user:42")
				if err != nil {
					log.Printf("[工人%d] 限流器报错了: %v", workerID, err)
					continue
				}
				if allowed {
					pass++
					log.Printf("[工人%d] 第%d次请求：放行", workerID, i+1)
				} else {
					block++
					log.Printf("[工人%d] 第%d次请求：被限流", workerID, i+1)
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

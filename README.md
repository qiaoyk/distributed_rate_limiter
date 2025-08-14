# rate_limiter
my rate limiter for distributed service, just for fun

A distributed flow limiter with fallback policy 
The main limiter uses a token bucket in redis, when the main limiter goes down, the memory based limiter will be enabled, when the health check loop finds the main limiter recovering, it switches to the main limiter again 
The memory based limiter uses a [time.rate]https://pkg.go.dev/golang.org/x/time/rate implementation

The fallback limiter, in-memory limiter, and redis distributed limiter can all be used independently.

一个带有回退策略的分布式限流器
主限流器使用令牌桶基于redis实现，当主限流器宕机，启用基于内存的单机限流，当health check loop发现主限流器恢复，再次切换到主限流器
基于内存的限流器使用了[time.rate]https://pkg.go.dev/golang.org/x/time/rate实现

回退式限流器，单机内存限流器，redis分布式限流器均可独立使用

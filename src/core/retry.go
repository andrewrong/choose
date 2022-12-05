package core

import (
	"context"
	"log"
	"math/rand"
	"time"
)

func Retry(logger *log.Logger, retryInterval, timeout time.Duration, maxRetry int, fn func() (bool, error)) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errChan := make(chan bool)
	go func() {
		start := time.Now()
		var realRetryInterval time.Duration

		var retry int
		for retry <= maxRetry {
			res, err := fn()
			if err == nil {
				errChan <- res
				return
			}

			realRetryInterval = time.Duration(func(min, max int64) int64 {
				if min == 0 {
					return min
				}
				return rand.Int63n(max-min) + min
			}(int64(retryInterval), int64(retryInterval)*2))
			// 如果接下来的等待的时间 + 之前的重试消耗时间 >= timeout 就不需要等待了，直接返回即可
			if (time.Since(start) + realRetryInterval) >= timeout {
				errChan <- false
				return
			}

			logger.Printf("retry(%d) after %v", retry, realRetryInterval)
			time.Sleep(realRetryInterval)
			retryInterval = retryInterval * 2
			retry++
		}
		errChan <- false
		return
	}()

	select {
	case <-ctx.Done():
		{
			logger.Printf("retry timeout")
			return false
		}
	case res := <-errChan:
		{
			return res
		}
	}
}

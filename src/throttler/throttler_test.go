package throttler_test

import (
	"testing"
	"throttler"
	"time"
)

func makeFakeTicker(c <-chan time.Time) throttler.TimeSource {
	return func(d time.Duration) (<-chan time.Time) {
		return c
	}
}
func TestTokenBucket(t *testing.T) {
	testTicker := make(chan time.Time)
	bucket := throttler.MakeTokenBucket(5, 10, 1000, makeFakeTicker(testTicker))
	resultCh := make(chan bool)
	
	bucket.TokenChannel() <- throttler.MakeTokenRequest(resultCh, 10)
	if res := <-resultCh; !res {
		t.Error("error getting 10 tokens from 10-token bucket")
	}
	
	bucket.TokenChannel() <- throttler.MakeTokenRequest(resultCh, 10)
	if res := <-resultCh; res {
		t.Error("able to get 10 tokens from empty bucket")
	}

	// refill 5 tokens each time, total 10
	testTicker <- time.Now()
	testTicker <- time.Now()
	bucket.TokenChannel() <- throttler.MakeTokenRequest(resultCh, 10)
	if res := <-resultCh; !res {
		t.Error("error getting 10 tokens from refilled bucket")		
	}
}



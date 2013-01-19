package throttler

import (
	"math"
	"time"
)

type TokenRequest struct {
	returnCh        chan bool
	requestedTokens float64
}

type TimeSource func(d time.Duration) <-chan time.Time

// http://en.wikipedia.org/wiki/Token_bucket
type TokenBucket struct {
	refillRate float64 // per second
	numTokens  float64
	capacity   float64
	timeSource TimeSource
	stopCh     chan bool
	tokenCh    chan *TokenRequest
}

func MakeTokenRequest(c chan bool, requestedTokens float64) *TokenRequest {
	return &TokenRequest{c, requestedTokens}
}

func MakeTokenBucket(refillRate float64, capacity float64,
	refillMs int64, timeSource TimeSource) *TokenBucket {
	tb := &TokenBucket{refillRate, capacity, capacity, timeSource,
		make(chan bool),
		make(chan *TokenRequest)}
	tb.start(refillMs)
	return tb
}

func (tb *TokenBucket) start(refillMs int64) {
	numRefill := float64(refillMs) / 1000.0
	go func() {
		for {
			select {
			case req := <-tb.tokenCh:
				tb.processRequest(req)
			case <-tb.stopCh:
				tb.cleanup()
				return
			case <-tb.timeSource(time.Duration(refillMs) * time.Millisecond):
				tb.refillTokens(numRefill)
			}
		}
	}()
}

func (tb *TokenBucket) processRequest(req *TokenRequest) {
	if tb.numTokens < req.requestedTokens {
		req.returnCh <- false
	} else {
		tb.numTokens -= req.requestedTokens
		req.returnCh <- true
	}
}

func (tb *TokenBucket) refillTokens(refillRatio float64) {
	totTokens := tb.numTokens + (tb.refillRate * refillRatio)
	tb.numTokens = math.Min(totTokens, tb.capacity)
}

func (tb *TokenBucket) cleanup() {
	close(tb.tokenCh)
	close(tb.stopCh)
}

func (tb *TokenBucket) TokenChannel() chan<- *TokenRequest {
	return tb.tokenCh
}

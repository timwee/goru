package estimator

import (
	"math"
)

// interface for various rate estimators
type RateEstimator interface {
	LogEvent(t float64, val float64)
	Rate(t float64) float64
}

// exponentially decaying estimator
//   alpha - "bandwidth" parameter. equivalent to
//               estimate of when something decays by 1/e in time units
//               if this were half-life estimator instead, this parameter
//               would just be the half-life (when the thing you are estimating
//               halves)
//  last - the previous observation's timestamp. initially set to time when
//               estimator is created
//  s - sum of values so far (this gets decayed and added to on LogEvent, as time passes)
//  w - sum of time so far (this gets decayed and added to on LogEvent, as time passes)

type ExponentialRateEstimator struct {
	alpha float64 
	last float64
	s float64
	w float64
}

// an estimator that uses halflife parameter to exponentially decay
// Since humans are better at estimating halflife(when something halves), than when something
//	becomes 1/e.
//
// The following discussion assumes 1 time unit passed.
// In the normal exponentialRateEstimator, we decay the old value by e ^ -(1/alpha).
//   Alpha is the time at which an older sample is decayed by 1/e.
// In HFExponentialEstimator, since our parameter is when a sample decays by 1/2,
//   this means that (2) ^ -(1/ halflife) is equivalent to e ^ - (1/alpha)
type HfExponentialRateEstimator struct {
	p float64 // will be (1/2) ^ (1/halflife)
	last float64
	s float64
	w float64
}

// alpha is a time constant, at which an older sample is discounted to 1/e relative to current data
func MakeExpRateEstimator(alpha float64, t float64) *ExponentialRateEstimator {
	res := new(ExponentialRateEstimator)
	res.alpha = alpha
	res.last = t
	res.s = 0
	res.w = 0
	return res
}

// hf is when the thing you want to estimate will decay in half (halflife)
func MakeHfExpRateEstimator(hf float64, t float64) *HfExponentialRateEstimator {
	res := new(HfExponentialRateEstimator)
	res.p = math.Exp2(-1.0 / hf)
	res.last = t
	res.s = 0
	res.w = 0
	return res
}

func (e *ExponentialRateEstimator) decay(tu float64) {
	pi := math.Exp((-1 * (tu - e.last)) / e.alpha)
	e.s = e.s * pi 
	e.w = e.w * pi
	e.last = tu	
}	

// Log event val, at time t
func (e *ExponentialRateEstimator) LogEvent(t float64, val float64) {
	e.decay(t)
	e.s += val
	e.w += 1
}

// get the current rate
func (e *ExponentialRateEstimator) Rate(t float64) float64 {
//	e.decay(t)
	return e.s / e.w
}

// decay the rate estimate so far, using tu as the reference/current time
func (e *HfExponentialRateEstimator) decay(tu float64) {
	pi := math.Pow(e.p, tu - e.last)
	e.s = e.s * pi 
	e.w = e.w * pi
	e.last = tu
}	

func (e *HfExponentialRateEstimator) LogEvent(t float64, val float64) {
	e.decay(t)
	e.s += val
	e.w += 1
}

func (e *HfExponentialRateEstimator) Rate(t float64) float64 {
//	e.decay(t)
	return e.s / e.w
}


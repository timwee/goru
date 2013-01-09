package estimator

import (
	"testing"
	"math"
	"fmt"
)

func TestExponentialRateEstimator(t *testing.T) {
	re := MakeExpRateEstimator(5, 0.0)
	ti := []float64{11.35718, 21.54637, 28.91061, 33.03586, 39.57767}
	x := []float64{1.5992071, -1.3577032, -0.3405638, 0.7048632, 0.3020558}
	m := []float64{1.5992071, -1.0168100, -0.4797436, 0.2836447, 0.2966159}

	for i, e := range ti {
		fmt.Println(i,e)
		re.LogEvent(e, x[i])
		est := re.Rate(0.0)
		if (math.Abs(m[i] - est) > 0.00001) {
			t.Error("failed for iteration %s, expected was %s, actual was %s", i, m[i], est)
		}
	}
}

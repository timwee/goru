package cardinality

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"
	)

var InvalidPError = errors.New("p has to be between 4 and 64")
var POW_2_64 float64 = 18446744073709551616.0
var POW_NEG_2_64 float64 = -18446744073709551616.0

type HLL struct {
	registers []int32
	m uint64
	p uint64
	alphaM float64
	h hash.Hash64
}

func Nlz64(x uint64) int32 {
	n := uint64(64)
	c := uint64(32)
	if x == 0 {
		return 64
	}
	for c != 0 {
		y := x >> c
		if y != 0 {
			n -= c
			x = y
		}
		c >>= 1
	}
	return int32(n - x)
}

func ShiftedNlz64(x uint64, shift uint64) int32 {
	return Nlz64((x << shift) | 1 << (shift - 1))
}

func MakeHLL(p uint64) (*HLL, error) {
	if p < 4 || p > 64 {
		return nil, InvalidPError
	}
	m := uint64(1 << p)
	f_m := float64(m)
	registers := make([]int32, m, m)
	alphaM := 0.0
	switch p {
	case 4:	alphaM = 0.673 * f_m * f_m
	case 5:	alphaM = 0.697 * f_m * f_m
	case 6:	alphaM = 0.709 * f_m * f_m
	default: alphaM = (0.7213 / (1 + 1.079 / f_m)) * f_m * f_m
	}
	return &HLL{registers, m, p, alphaM, fnv.New64()}, nil
}

func (hll *HLL) Observe(d []byte) {
	hll.h.Reset()
	hll.h.Write(d)
	hash := hll.h.Sum64()
	// r is the register that we will update. R is between 0 and 2^p
	r := hash >> (64 - hll.p)

	nlz := ShiftedNlz64(hash, hll.p)
	if hll.registers[r] < nlz {
		hll.registers[r] = nlz
	}
}

func (hll *HLL) Estimate() int64 {
	harmonic_m := 0.0
	num_0 := 0
	for _, r := range(hll.registers) {
		harmonic_m += math.Pow(2, float64(-1 * r))
		if r == 0 {
			num_0 += 1
		}
	}
	est := hll.alphaM * (1.0 / harmonic_m)

	if est <= (5.0/2.0 * float64(hll.m)) {
		if num_0 != 0 {
			est = LinearCounting(hll.m, float64(num_0))
		}
		// est = est
	} else if est > (1/30 * POW_2_64) { // revisit 1/30
		est = POW_NEG_2_64 * math.Log(1 - (est / POW_2_64))
	}
	return int64(math.Ceil(est))
}

func LinearCounting(m uint64, v float64) float64 {
	return math.Ceil(float64(m) * math.Log(float64(m) / v))
}
	

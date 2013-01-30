package hhitter

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"
	"time"
)

var ErrElementNotFound = errors.New("cms: element not found")
var LOWER32_MASK uint64 = ^uint64(0) >> 32
var MAX_FLOAT64 float64 = math.Inf(1)

type UpdateFn func(elem *CMElement, update *CMUpdate) *CMElement
type ReadFn func(elem *CMElement, readTime time.Time) float64

// implementation of CMS (count min sketch), optionally exponentially decayed
type CountMin struct {
	matrix     [][]CMElement
	numBuckets uint32      // better to be prime
	k          uint32      // num hashes
	h          hash.Hash64 // hash to use, we use a + b * i, where i in (0,k], and a and b are obtained from hash h's upper 32 and lower 32 bits
	updateFn   UpdateFn
	readFn     ReadFn
}

type CMUpdate struct {
	Key        []byte    // key
	Weight     float64   // weight
	UpdateTime time.Time // time of update
}

type CMElement struct {
	Last_update time.Time
	Weight      float64
}

func exp_decay(decay float64, prev time.Time, cur time.Time, weight float64) float64 {
	return math.Exp(decay*(cur.Sub(prev).Seconds())) * weight
}

func expRead_from_decay(decay float64) ReadFn {
	return func(elem *CMElement, readTime time.Time) float64 {
		return exp_decay(decay, elem.Last_update, readTime, elem.Weight)
	}
}

func Plain_read(elem *CMElement, readTime time.Time) float64 {
	return elem.Weight
}

func expUpdateFn_from_decay(decay float64) UpdateFn {
	return func(elem *CMElement, update *CMUpdate) (result *CMElement) {
		// we don't decay if it is 0
		if elem.Weight > 0.00000001 {
			elem.Weight = exp_decay(decay, elem.Last_update, update.UpdateTime, elem.Weight)
		}
		elem.Weight += update.Weight
		elem.Last_update = update.UpdateTime
		return elem
	}
}

func Plain_update(elem *CMElement, update *CMUpdate) *CMElement {
	elem.Weight += update.Weight
	return elem
}

func MakeCMSDirect(size uint32, numHash uint32, seed int64, updateF UpdateFn, readF ReadFn) *CountMin {
	mat := make([][]CMElement, numHash)
	for i, _ := range mat {
		mat[i] = make([]CMElement, size)
	}
	return &CountMin{mat, size, numHash, fnv.New64(), updateF, readF}
}

func MakeCMS(eps float64, p_error float64, seed int64) *CountMin {
	size, numHashes := estimate(eps, p_error)
	return MakeCMSDirect(size, numHashes, seed, Plain_update, Plain_read)
}

func MakeExpCMS(eps float64, p_error float64, seed int64, decay float64) *CountMin {
	size, numHashes := estimate(eps, p_error)
	return MakeCMSDirect(size, numHashes, seed, expUpdateFn_from_decay(decay), expRead_from_decay(decay))
}

func estimate(eps float64, p_error float64) (size uint32, numHashes uint32) {
	size = uint32(math.Ceil(2 / eps))
	numHashes = uint32(math.Ceil(math.Log2(1 / p_error)))
	return
}

func (cms *CountMin) getHashParams(data []byte) (uint32, uint32) {
	cms.h.Reset()
	cms.h.Write(data)
	sum := cms.h.Sum64()
	lower := uint32(sum & LOWER32_MASK)
	upper := uint32((sum >> 32) & LOWER32_MASK)
	return lower, upper
}

func (cms *CountMin) getBuckets(data []byte) []uint32 {
	buckets := make([]uint32, cms.k, cms.k)
	a, b := cms.getHashParams(data)
	for i := uint32(0); i < cms.k; i++ {
		buckets[i] = (a + (b * i)) % cms.numBuckets
	}
	return buckets
}

// get estimated count
func (cms *CountMin) Count(data []byte) (min float64, err error) {
	return cms.CountT(data, time.Now())
}

func (cms *CountMin) CountT(data []byte, readTime time.Time) (min float64, err error) {
	min = MAX_FLOAT64
	for i, b := range cms.getBuckets(data) {
		cur := cms.readFn(&cms.matrix[i][b], readTime)
		if min > cur {
			min = cur
		}
	}
	if min == MAX_FLOAT64 {
		return 0, ErrElementNotFound
	}
	return min, nil
}

// increments and returns estimated count so far
func (cms *CountMin) Update(data []byte, weight float64) float64 {
	return cms.UpdateT(data, weight, time.Now())
}

func (cms *CountMin) UpdateT(data []byte, weight float64, updateTime time.Time) float64 {
	update := &CMUpdate{data, weight, updateTime}
	min := MAX_FLOAT64
	for i, b := range cms.getBuckets(data) {
		cur := &cms.matrix[i][b]
		cur = cms.updateFn(cur, update)
		if min > cur.Weight {
			min = cur.Weight
		}
	}
	return min
}

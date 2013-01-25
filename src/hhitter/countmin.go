package hhitter

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"
)

var ErrElementNotFound = errors.New("cms: element not found")
var MAX_INT32 int32 = int32(^uint(0) >> 1)
var LOWER32_MASK uint64 = ^uint64(0) >> 32

// implementation of CMS (count min sketch)
type CountMin struct {
	matrix     [][]int32
	numBuckets uint32      // better to be prime
	k          uint32      // num hashes
	h          hash.Hash64 // hash to use, we use a + b * i, where i in (0,k], and a and b are obtained from hash h's upper 32 and lower 32 bits
}

func MakeCMSDirect(size uint32, numHash uint32, seed int64) *CountMin {
	mat := make([][]int32, numHash)
	for i, _ := range mat {
		mat[i] = make([]int32, size)
	}
	return &CountMin{mat, size, numHash, fnv.New64()}
}

func MakeCMS(eps float64, p_error float64, seed int64) *CountMin {
	size := uint32(math.Ceil(2 / eps))
	numHashes := uint32(math.Ceil(math.Log2(1 / p_error)))
	return MakeCMSDirect(size, numHashes, seed)
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
func (cms *CountMin) Count(data []byte) (min int32, err error) {
	min = MAX_INT32
	for i, b := range cms.getBuckets(data) {
		cur := cms.matrix[i][b]
		if min > cur {
			min = cur
		}
	}
	if min == MAX_INT32 {
		return 0, ErrElementNotFound
	}
	return min, nil
}

// increments and returns min count
func (cms *CountMin) Update(data []byte, cnt int32) int32 {
	min := MAX_INT32
	for i, b := range cms.getBuckets(data) {
		cms.matrix[i][b] += cnt
		if min > cms.matrix[i][b] {
			min = cms.matrix[i][b]
		}
	}
	return min
}

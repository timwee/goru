package hhitter

import (
	"errors"
	"hash"
	"hash/fnv"
)

var ErrElementNotFound = errors.New("cms: element not found")
var MAX_INT32 int32 = int32(^uint(0) >> 1)
var LOWER32_MASK uint64 = ^uint64(0) >> 32

// implementation of CMS (count min sketch)
type CountMin struct {
	matrix     []int32 // flattened matrix
	numBuckets uint32
	k          uint32      // num hashes
	h          hash.Hash64 // hash to use, we use a + b * i, where i in (0,k], and a and b are obtained from hash h's upper 32 and lower 32 bits
}

func MakeCMS(size uint32, numHash uint32, seed int32) *CountMin {
	mat := make([]int32, numHash*size)
	return &CountMin{mat, size, numHash, fnv.New64()}
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
	buckets := make([]uint32, cms.k)
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
		cur := cms.matrix[uint32(b)+(uint32(i)*cms.numBuckets)]
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
func (cms *CountMin) Inc(data []byte) int32 {
	min := MAX_INT32
	for i, b := range cms.getBuckets(data) {
		idx := uint32(b) + (uint32(i) * cms.numBuckets)
		cms.matrix[idx] += 1
		if min > cms.matrix[idx] {
			min = cms.matrix[idx]
		}
	}
	return min
}

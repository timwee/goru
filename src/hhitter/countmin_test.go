package hhitter_test

import (
	"encoding/binary"
	"fmt"
	"hhitter"
	"math"
	"math/rand"
	"testing"
)

func TestCMSSanity(t *testing.T) {
	cms := hhitter.MakeCMSDirect(100, 100, 2.0)
	k1 := []byte("hello")
	cnt, _ := cms.Count(k1)
	if 0 != cnt {
		t.Error("shouldn't be anything yet")
	}

	for i := 0; i < 10; i++ {
		cms.Update(k1, 1)
	}
	cnt, _ = cms.Count(k1)
	if cnt != 10 {
		t.Error("should have 10")
	}
}

func TestCMSAccuracy(t *testing.T) {
	seed := int64(7364181)
	rand.Seed(seed)
	msb := int32(20)
	numItems := 1000000
	items := make([]int32, numItems)
	cms := hhitter.MakeCMS(0.0001, 0.01, seed)
	for i, _ := range items {
		next_msb := uint32(rand.Int31n(msb))
		items[i] = rand.Int31n(int32(1 << next_msb))
		cms.Update(intToBuf(items[i]), 1)
	}

	actual := make([]int32, 1<<uint32(msb))
	for _, x := range items {
		actual[x] += 1
	}

	numErrors := 0
	for item, cnt := range actual {
		est, _ := cms.Count(intToBuf(int32(item)))
		diff := math.Abs(float64(est-cnt)) / float64(numItems)
		if diff > 1.0001 {
			numErrors += 1
		}
	}

	if errorRate := float64(numErrors) / float64(len(actual)); errorRate > 0.01 {
		t.Errorf("errorRate %d  > 0.01", errorRate)
	}
}

func intToBuf(data int32) []byte {
	buf := make([]byte, 8)
	if wrote := binary.PutVarint(buf, int64(data)); wrote < 1 {
		panic(fmt.Sprintf("failed to write %i in CMS insert, was %i", data, wrote))
	}
	return buf
}

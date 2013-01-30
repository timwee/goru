package hhitter_test

import (
	"encoding/binary"
	"fmt"
	"hhitter"
	"math"
	"math/rand"
	"testing"
	"time"
)

var eps float64 = 0.0001

func TestCMSSanity(t *testing.T) {
	cms := hhitter.MakeCMSDirect(100, 100, 2.0, hhitter.Plain_update, hhitter.Plain_read)
	k1 := []byte("hello")
	cnt, _ := cms.Count(k1)
	if 0 != cnt {
		t.Error("shouldn't be anything yet")
	}

	for i := 0; i < 10; i++ {
		cms.Update(k1, 1.0)
	}
	cnt, _ = cms.Count(k1)
	if math.Abs(cnt-10.0) > eps {
		t.Error("should have 10")
	}
}

func TestCMSExpSanity(t *testing.T) {
	now := time.Now()
	k1 := []byte("hello")
	cms := hhitter.MakeExpCMS(0.001, 0.0001, 5, 0.1)
	cms.UpdateT(k1, 1.0, now)
	cnt, _ := cms.CountT(k1, now)

	if math.Abs(cnt-1.0) > eps {
		t.Errorf("cnt should be 1.0, instead was %d", cnt)
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
		cms.Update(intToBuf(items[i]), float64(1))
	}

	actual := make([]int32, 1<<uint32(msb))
	for _, x := range items {
		actual[x] += 1
	}

	numErrors := 0
	for item, cnt := range actual {
		est, _ := cms.Count(intToBuf(int32(item)))
		diff := math.Abs(est-float64(cnt)) / float64(numItems)
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

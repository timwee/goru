package hhitter_test

import (
	"hhitter"
	"testing"
	//	"fmt"
)

func TestCMSSanity(t *testing.T) {
	cms := hhitter.MakeCMS(100, 100, 2.0)
	k1 := []byte("hello")
	cnt, _ := cms.Count(k1)
	if 0 != cnt {
		t.Error("shouldn't be anything yet")
	}

	for i := 0; i < 10; i++ {
		cms.Inc(k1)
	}
	cnt, _ = cms.Count(k1)
	if cnt != 10 {
		t.Error("should have 10")
	}
}

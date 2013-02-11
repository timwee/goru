package cardinality_test

import (
	"cardinality"
	"testing"

)

func TestHLLSanity(t *testing.T) {
}

func TestNlz64(t *testing.T) {
	x := uint64(1) << 31
	if nlz := cardinality.Nlz64(x); nlz != 32 {
		t.Errorf("expected 32, but was %d", nlz)
	}
	x = uint64(0)
	if nlz := cardinality.Nlz64(x); nlz != 64 {
		t.Errorf("expected %d, but was %d", 64, nlz)
	}
	x = uint64(1) << 63
	if nlz := cardinality.Nlz64(x); nlz != 0 {
		t.Errorf("expected %d, but was %d", 0, nlz)
	}
}

func TestShiftedNlz64(t *testing.T) {
	shift := uint64(5)
	x := uint64(0)
	if s_nlz := cardinality.ShiftedNlz64(x, shift); s_nlz != 59 {
		t.Errorf("expected %d, but got %d", 59, s_nlz)
	}
	x = uint64(1 << 31)
	if s_nlz := cardinality.ShiftedNlz64(x, shift); s_nlz != 27 {
		t.Errorf("expected %d, but got %d", 27, s_nlz)
	}
}

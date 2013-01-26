package topk_test

import (
	"testing"
	"topk"
)

func TestSpaceSavingSanity(t *testing.T) {
	ss := topk.MakeSpaceSaving(4)
	for i := 0; i < 10; i++ {
		ss.Observe("a")
	}
	c := ss.GetCounter("a")
	if c.GetCount() != 10 {
		t.Errorf("expected 'a' to have count of 10, but was %i", c.GetCount())
	}
}

func TestTopK(t *testing.T) {
	ss := topk.MakeSpaceSaving(4)
	xs := []string{"a", "b", "c", "d", "a", "b", "e", "a", "b"}
	for _, s := range xs {
		ss.Observe(s)
	}
	k := 2
	topk, _ := ss.TopK(int32(k))
	if len(topk) != 2 {
		t.Errorf("result didn't have %d elements, instead was %d", k, len(topk))
	}

	for _, c := range topk {
		if c.Key != "a" && c.Key != "b" {
			t.Errorf("key of top2 not 'a' or 'b' was %s", c.Key)
		}
	}
}

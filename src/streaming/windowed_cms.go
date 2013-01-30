package streaming

import (
	"container/list"
	"errors"
)

type WindowedCMS struct {
	sketches *list.List
	Counter  int64
	limit    int64
}

// makes window/sliceLength sketches. The sketches use the eps, p_error, and seed params
//  all the sketches are the same
func MakeWindowedCMS(eps float64, p_error float64, seed int64, window int64, sliceLength int64) (*WindowedCMS, error) {
	if sliceLength <= 0 || window <= 0 || window < sliceLength {
		return nil, errors.New("invalid input to MakeWindowedCMS")
	}
	size, numHashes := estimate(eps, p_error)
	numSketches := window / sliceLength
	sketches := list.New()
	for i := int64(0); i < numSketches; i++ {
		sketches.PushFront(MakeCMSDirect(size, numHashes, seed, Plain_update, Plain_read))
	}
	return &WindowedCMS{sketches, 0, sliceLength}, nil
}

func (w *WindowedCMS) Update(data []byte, weight float64) {
	if w.Counter == w.limit {
		v := w.sketches.Remove(w.sketches.Front())
		w.sketches.PushBack(v)
		w.sketches.Front().Value.(*CountMin).Reset()
		w.Counter = 0
	}
	w.sketches.Front().Value.(*CountMin).Update(data, weight)
	w.Counter += 1
}

func (w *WindowedCMS) Count(data []byte) (float64, error) {
	cnt := 0.0
	for e := w.sketches.Front(); e != nil; e = e.Next() {
		sketch := e.Value.(*CountMin)
		c, _ := sketch.Count(data)
		cnt += c
	}
	return cnt, nil
}

func (w *WindowedCMS) Reset() {
	for e := w.sketches.Front(); e != nil; e = e.Next() {
		e.Value.(*CountMin).Reset()
	}
}

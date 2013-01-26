package topk

import (
	"container/list"
)

// based on Efficient Computation of Frequent and Top-k Elements in Data Streams
//  Metwally, Agrawal, and Abbadi
// uses linkedlist version (alternative to heap)
// According to experiments from
//   "Finding Frequent items in Data Streams (Cormode, Hadjieleftheriou)"
//   linkedlist is faster
type SpaceSaving struct {
	counterMap  map[interface{}]*list.Element
	maxCounters int32
	buckets     *list.List
}

type Bucket struct {
	counters *list.List
	count    int64
}

type Counter struct {
	Key    interface{}
	error  int64
	count  int64
	bucket *list.Element
}

func MakeSpaceSaving(numCounters int32) *SpaceSaving {
	return &SpaceSaving{make(map[interface{}]*list.Element), numCounters, list.New()}
}

func (c *Counter) GetCount() int64 {
	return c.count
}

func (ss *SpaceSaving) Observe(value interface{}) *Counter {
	counterNode := ss.counterMap[value]
	if counterNode == nil {
		if int32(len(ss.counterMap)) < ss.maxCounters {
			counter, counterElem := ss.initCounter(value)
			ss.counterMap[value] = counterElem
			return counter
		}
		minBucket := ss.buckets.Front().Value.(*Bucket)
		minCounterElem := minBucket.counters.Front()
		minCounter := minCounterElem.Value.(*Counter)

		delete(ss.counterMap, minCounter.Key)
		minCounter.Key = value
		minCounter.error = minBucket.count
		ret := ss.incrementCounter(minCounterElem)
		ss.counterMap[value] = ret
		return ret.Value.(*Counter)
	}
	return ss.incrementCounter(counterNode).Value.(*Counter)
}

func (ss *SpaceSaving) incrementCounter(counterElem *list.Element) *list.Element {
	counter := counterElem.Value.(*Counter)
	curBucketElem := counter.bucket
	curBucket := curBucketElem.Value.(*Bucket)
	curBucket.counters.Remove(counterElem)

	toInsertBucketElem := counter.bucket.Next()
	counter.count += 1

	if toInsertBucketElem == nil || toInsertBucketElem.Value.(*Bucket).count != counter.count {
		toInsertBucketElem = ss.buckets.InsertAfter(&Bucket{list.New(), counter.count}, curBucketElem)
	}
	toInsertBucket := toInsertBucketElem.Value.(*Bucket)

	counter.bucket = toInsertBucketElem
	result := toInsertBucket.counters.PushBack(counter)

	if curBucket.counters.Len() == 0 {
		ss.buckets.Remove(curBucketElem)
	}
	return result
}

func (ss *SpaceSaving) initCounter(value interface{}) (*Counter, *list.Element) {
	minBucket := ss.buckets.Front()
	if minBucket == nil || minBucket.Value.(*Bucket).count != 1 {
		counters := list.New()
		toInsert := &Bucket{counters, 1}
		bucketElem := ss.buckets.PushFront(toInsert)
		result := &Counter{value, 0, 1, bucketElem}
		resultElem := toInsert.counters.PushFront(result)
		return result, resultElem
	} else {
		result := &Counter{value, 0, 1, minBucket}
		resultElem := minBucket.Value.(*Bucket).counters.PushBack(result)
		return result, resultElem
	}
	return nil, nil
}

// mainly for testing
func (ss *SpaceSaving) GetCounter(value interface{}) *Counter {
	elem := ss.counterMap[value]
	if elem != nil {
		return elem.Value.(*Counter)
	}
	return nil
}

func (ss *SpaceSaving) TopK(k int32) ([]*Counter, int32) {
	curBucketElem := ss.buckets.Back()
	result := make([]*Counter, k, k)
	i := int32(0)
	for curBucketElem != nil && i < k {
		curBucket := curBucketElem.Value.(*Bucket)
		curCounterElem := curBucket.counters.Front()
		for curCounterElem != nil && i < k {
			result[i] = curCounterElem.Value.(*Counter)
			curCounterElem = curCounterElem.Next()
			i += 1
		}
		curBucketElem = curBucketElem.Next()
	}
	return result, i
}

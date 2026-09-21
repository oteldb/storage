package mergestream

import "github.com/oteldb/storage/signal"

// Source is one merge input's key sequence, ascending and addressed by index. Repeated keys are
// allowed; [Keys] collapses them.
type Source interface {
	// Len returns how many keys the source holds.
	Len() int
	// At returns the i-th key.
	At(i int) signal.SeriesID
}

// SeriesIDs adapts an ascending id slice to [Source].
type SeriesIDs []signal.SeriesID

// Len implements [Source].
func (s SeriesIDs) Len() int { return len(s) }

// At implements [Source].
func (s SeriesIDs) At(i int) signal.SeriesID { return s[i] }

// Keys yields the ascending union of several [Source] key sequences through a k-way heap, so a
// merge visits each key once without ever holding a set of them: the state is O(sources), not
// O(distinct keys), and no key is hashed or sorted.
//
// The zero value is ready to [Keys.Reset]; a reset reuses the heap, so a reused Keys allocates
// nothing per traversal.
type Keys struct {
	heap []cursor
	key  signal.SeriesID
}

type cursor struct {
	src  Source
	key  signal.SeriesID
	i, n int
}

// Reset arms k over src, discarding any traversal in progress. Each source must be ascending.
func (k *Keys) Reset(src []Source) {
	k.heap = k.heap[:0]
	k.key = signal.SeriesID{}

	for _, s := range src {
		n := s.Len()
		if n == 0 {
			continue
		}

		k.heap = append(k.heap, cursor{src: s, key: s.At(0), n: n})
	}

	for i := len(k.heap)/2 - 1; i >= 0; i-- {
		k.down(i)
	}
}

// Next advances to the next distinct key, reporting false once every source is exhausted.
func (k *Keys) Next() bool {
	if len(k.heap) == 0 {
		return false
	}

	k.key = k.heap[0].key
	for len(k.heap) > 0 && k.heap[0].key == k.key {
		k.skip()
	}

	return true
}

// Key returns the key [Keys.Next] last advanced to.
func (k *Keys) Key() signal.SeriesID { return k.key }

// Append drains the remaining union onto dst.
func (k *Keys) Append(dst []signal.SeriesID) []signal.SeriesID {
	for k.Next() {
		dst = append(dst, k.key)
	}

	return dst
}

// skip advances the root cursor past every entry equal to the current key, dropping it when that
// exhausts it. A source's repeats are adjacent because it is ascending.
func (k *Keys) skip() {
	c := &k.heap[0]

	for c.i++; c.i < c.n; c.i++ {
		if key := c.src.At(c.i); key != k.key {
			c.key = key
			k.down(0)

			return
		}
	}

	last := len(k.heap) - 1
	k.heap[0] = k.heap[last]
	k.heap[last] = cursor{}
	k.heap = k.heap[:last]
	k.down(0)
}

func (k *Keys) down(i int) {
	h := k.heap

	for {
		l := 2*i + 1
		if l >= len(h) {
			return
		}

		m := l
		if r := l + 1; r < len(h) && h[r].key.Compare(h[l].key) < 0 {
			m = r
		}

		if h[m].key.Compare(h[i].key) >= 0 {
			return
		}

		h[i], h[m] = h[m], h[i]
		i = m
	}
}

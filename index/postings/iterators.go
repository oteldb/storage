package postings

import (
	"sort"

	"github.com/oteldb/storage/signal"
)

// Postings is a forward iterator over a sorted, deduplicated stream of [signal.SeriesID].
// Next advances; Seek jumps to the first id ≥ v; At returns the current id; Err reports a
// terminal error. The set-op constructors ([Intersect], [Merge], [Without]) compose
// Postings lazily, so resolving a matcher never materializes intermediate lists.
type Postings interface {
	Next() bool
	Seek(v signal.SeriesID) bool
	At() signal.SeriesID
	Err() error
}

// Empty returns a Postings with no entries.
func Empty() Postings { return emptyPostings{} }

type emptyPostings struct{}

func (emptyPostings) Next() bool                { return false }
func (emptyPostings) Seek(signal.SeriesID) bool { return false }
func (emptyPostings) At() signal.SeriesID       { return signal.SeriesID{} }
func (emptyPostings) Err() error                { return nil }

// FromSlice returns a Postings over a sorted, deduplicated slice (not copied).
func FromSlice(s []signal.SeriesID) Postings {
	if len(s) == 0 {
		return Empty()
	}

	return &slicePostings{s: s, i: -1}
}

// ToSlice fully drains a Postings into a slice (for tests and materialization).
func ToSlice(p Postings) ([]signal.SeriesID, error) {
	var out []signal.SeriesID
	for p.Next() {
		out = append(out, p.At())
	}

	return out, p.Err()
}

type slicePostings struct {
	s []signal.SeriesID
	i int
}

func (p *slicePostings) Next() bool { p.i++; return p.i < len(p.s) }

func (p *slicePostings) Seek(v signal.SeriesID) bool {
	if p.i < 0 {
		p.i = 0
	}

	if p.i >= len(p.s) {
		return false
	}

	if !p.s[p.i].Less(v) { // already at or past v
		return true
	}

	base := p.i
	p.i = base + sort.Search(len(p.s)-base, func(j int) bool { return !p.s[base+j].Less(v) })

	return p.i < len(p.s)
}

func (p *slicePostings) At() signal.SeriesID { return p.s[p.i] }
func (p *slicePostings) Err() error          { return nil }

// Intersect returns the set intersection (AND) of the inputs.
func Intersect(ps ...Postings) Postings {
	switch len(ps) {
	case 0:
		return Empty()
	case 1:
		return ps[0]
	default:
		return &intersectPostings{arr: ps}
	}
}

type intersectPostings struct {
	arr     []Postings
	cur     signal.SeriesID
	started bool
}

func (it *intersectPostings) Next() bool {
	if !it.started {
		it.started = true
		for _, p := range it.arr {
			if !p.Next() {
				return false
			}
		}

		return it.align()
	}

	if !it.arr[0].Next() {
		return false
	}

	return it.align()
}

func (it *intersectPostings) Seek(v signal.SeriesID) bool {
	if it.started && !it.cur.Less(v) {
		return true
	}

	if !it.started {
		it.started = true
	}

	for _, p := range it.arr {
		if !p.Seek(v) {
			return false
		}
	}

	return it.align()
}

func (it *intersectPostings) At() signal.SeriesID { return it.cur }

func (it *intersectPostings) Err() error {
	for _, p := range it.arr {
		if err := p.Err(); err != nil {
			return err
		}
	}

	return nil
}

// align advances every input to a common id, or reports exhaustion.
func (it *intersectPostings) align() bool {
	for {
		maxID := it.arr[0].At()
		for _, p := range it.arr[1:] {
			if maxID.Less(p.At()) {
				maxID = p.At()
			}
		}

		equal := true
		for _, p := range it.arr {
			if p.At().Less(maxID) {
				if !p.Seek(maxID) {
					return false
				}
			}

			if p.At() != maxID {
				equal = false
			}
		}

		if equal {
			it.cur = maxID

			return true
		}
	}
}

// Merge returns the set union (OR) of the inputs, deduplicated.
func Merge(ps ...Postings) Postings {
	switch len(ps) {
	case 0:
		return Empty()
	case 1:
		return ps[0]
	default:
		return &mergePostings{arr: ps}
	}
}

// mergePostings is a k-way union over its inputs, ordered by a binary min-heap so the per-emitted-id
// cost is O(log k) (a heap re-sift) rather than O(k) (a linear min-scan): resolving a high-cardinality
// matcher — `__name__=~"node_.+"` over ~1300 metric-name buckets — drops from O(N×k) to O(N×log k).
// Each heap entry caches its input's current id, so a sift compares struct values without an At()
// interface call. Inputs are sorted+deduplicated, so the union is emitted sorted and deduplicated.
type mergePostings struct {
	arr     []Postings
	h       []mergeEntry // min-heap by key (== p.At()); built lazily on the first Next/Seek
	cur     signal.SeriesID
	started bool
}

// mergeEntry is one heap slot: an input and its current id (cached to avoid per-sift At() calls).
type mergeEntry struct {
	p   Postings
	key signal.SeriesID
}

func (m *mergePostings) Next() bool {
	if !m.started {
		m.started = true
		for _, p := range m.arr {
			if p.Next() {
				m.h = append(m.h, mergeEntry{p: p, key: p.At()})
			}
		}

		m.heapify()
	} else {
		// Dedup: advance every input sitting at the just-emitted id. They are exactly the heap roots
		// equal to cur, so advance the root (re-sifting it down, or removing it when exhausted) until
		// the root id differs — leaving the next distinct id at the root.
		for len(m.h) > 0 && m.h[0].key == m.cur {
			if m.h[0].p.Next() {
				m.h[0].key = m.h[0].p.At()
				m.siftDown(0)
			} else {
				m.removeRoot()
			}
		}
	}

	if len(m.h) == 0 {
		return false
	}

	m.cur = m.h[0].key

	return true
}

func (m *mergePostings) Seek(v signal.SeriesID) bool {
	switch {
	case m.started && !m.cur.Less(v):
		return true // already at or past v
	case !m.started:
		m.started = true
		m.seekInit(v)
	default:
		m.seekAdvance(v)
	}

	m.heapify()

	if len(m.h) == 0 {
		return false
	}

	m.cur = m.h[0].key

	return true
}

func (m *mergePostings) At() signal.SeriesID { return m.cur }

func (m *mergePostings) Err() error {
	for _, p := range m.arr {
		if err := p.Err(); err != nil {
			return err
		}
	}

	return nil
}

// seekInit seeds the heap on the first read, taking each input's first id ≥ v.
func (m *mergePostings) seekInit(v signal.SeriesID) {
	for _, p := range m.arr {
		if p.Seek(v) {
			m.h = append(m.h, mergeEntry{p: p, key: p.At()})
		}
	}
}

// seekAdvance moves every active input to its first id ≥ v, dropping those exhausted; the caller
// rebuilds the heap afterward.
func (m *mergePostings) seekAdvance(v signal.SeriesID) {
	w := 0

	for _, e := range m.h {
		if e.key.Less(v) {
			if !e.p.Seek(v) {
				continue
			}

			e.key = e.p.At()
		}

		m.h[w] = e
		w++
	}

	m.h = m.h[:w]
}

// heapify restores the min-heap invariant over the whole slice (after a bulk build or Seek rebuild).
func (m *mergePostings) heapify() {
	for i := len(m.h)/2 - 1; i >= 0; i-- {
		m.siftDown(i)
	}
}

// removeRoot drops the root (an exhausted input), moving the last entry up and re-sifting.
func (m *mergePostings) removeRoot() {
	last := len(m.h) - 1
	m.h[0] = m.h[last]
	m.h = m.h[:last]

	if len(m.h) > 0 {
		m.siftDown(0)
	}
}

// siftDown restores the heap invariant for the subtree rooted at i (the only entry that may be out
// of order), comparing the cached keys.
func (m *mergePostings) siftDown(i int) {
	n := len(m.h)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}

		c := l
		if r := l + 1; r < n && m.h[r].key.Less(m.h[l].key) {
			c = r
		}

		if !m.h[c].key.Less(m.h[i].key) {
			return
		}

		m.h[i], m.h[c] = m.h[c], m.h[i]
		i = c
	}
}

// Without returns the set difference a \ b (a AND NOT b).
func Without(a, b Postings) Postings { return &withoutPostings{a: a, b: b} }

type withoutPostings struct {
	a, b     Postings
	cur      signal.SeriesID
	bStarted bool
	bOK      bool
}

func (w *withoutPostings) Next() bool {
	for w.a.Next() {
		if !w.excluded(w.a.At()) {
			w.cur = w.a.At()

			return true
		}
	}

	return false
}

func (w *withoutPostings) Seek(v signal.SeriesID) bool {
	if !w.a.Seek(v) {
		return false
	}

	for {
		if !w.excluded(w.a.At()) {
			w.cur = w.a.At()

			return true
		}

		if !w.a.Next() {
			return false
		}
	}
}

func (w *withoutPostings) At() signal.SeriesID { return w.cur }

func (w *withoutPostings) Err() error {
	if err := w.a.Err(); err != nil {
		return err
	}

	return w.b.Err()
}

// excluded reports whether v is present in b, advancing b as needed.
func (w *withoutPostings) excluded(v signal.SeriesID) bool {
	if !w.bStarted {
		w.bStarted = true
		w.bOK = w.b.Next()
	}

	if w.bOK && w.b.At().Less(v) {
		w.bOK = w.b.Seek(v)
	}

	return w.bOK && w.b.At() == v
}

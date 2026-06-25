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

type mergePostings struct {
	arr     []Postings
	active  []Postings
	cur     signal.SeriesID
	started bool
}

func (m *mergePostings) Next() bool {
	if !m.started {
		m.started = true
		for _, p := range m.arr {
			if p.Next() {
				m.active = append(m.active, p)
			}
		}
	} else {
		m.advanceAtCur()
	}

	return m.pickMin()
}

func (m *mergePostings) Seek(v signal.SeriesID) bool {
	if m.started && !m.cur.Less(v) {
		return true
	}

	if !m.started {
		m.started = true
		for _, p := range m.arr {
			if p.Seek(v) {
				m.active = append(m.active, p)
			}
		}

		return m.pickMin()
	}

	w := 0
	for _, p := range m.active {
		if p.At().Less(v) {
			if !p.Seek(v) {
				continue
			}
		}

		m.active[w] = p
		w++
	}

	m.active = m.active[:w]

	return m.pickMin()
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

// advanceAtCur steps every input currently sitting at the last-emitted id.
func (m *mergePostings) advanceAtCur() {
	w := 0
	for _, p := range m.active {
		if p.At() == m.cur && !p.Next() {
			continue
		}

		m.active[w] = p
		w++
	}

	m.active = m.active[:w]
}

func (m *mergePostings) pickMin() bool {
	if len(m.active) == 0 {
		return false
	}

	minID := m.active[0].At()
	for _, p := range m.active[1:] {
		if p.At().Less(minID) {
			minID = p.At()
		}
	}

	m.cur = minID

	return true
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

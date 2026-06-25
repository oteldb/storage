// Package postings is the inverted index: for each (name, value) attribute it keeps the
// sorted list of [signal.SeriesID]s that carry it, and composes those lists with lazy
// set-op iterators ([Intersect]/[Merge]/[Without]) to resolve label matchers to series.
//
// Matching is **callback-based**: a [Matcher] carries a `Match func([]byte) bool`
// predicate, and [MemPostings.Select] scans a label's values applying it. Storage knows
// nothing about a query language's operators (regexp, equality, ranges) — a language
// package supplies the predicate (e.g. a compiled regexp's Match) and composes the
// primitives (Get/Select/ForName/WithoutName + the set ops) with its own semantics.
package postings

import (
	"slices"

	"github.com/oteldb/storage/signal"
)

// MemPostings is the in-memory inverted index: name → value → sorted series list, plus
// the set of all series. The zero value is not usable; create one with [NewMemPostings].
// Not safe for concurrent use; callers own synchronization (writes and reads must not
// overlap, as the first read sorts the lists in place).
type MemPostings struct {
	m      map[string]map[string][]signal.SeriesID
	all    []signal.SeriesID
	sorted bool
}

// NewMemPostings returns an empty index.
func NewMemPostings() *MemPostings {
	return &MemPostings{m: make(map[string]map[string][]signal.SeriesID)}
}

// Add records that series carries the attribute name=value. Lookups use the string([]byte)
// form, which the compiler keeps allocation-free; only a brand-new name or value key
// allocates.
func (p *MemPostings) Add(series signal.SeriesID, name, value []byte) {
	byVal := p.m[string(name)]
	if byVal == nil {
		byVal = make(map[string][]signal.SeriesID)
		p.m[string(name)] = byVal
	}

	byVal[string(value)] = append(byVal[string(value)], series)
	p.all = append(p.all, series)
	p.sorted = false
}

func sortDedup(s []signal.SeriesID) []signal.SeriesID {
	slices.SortFunc(s, signal.SeriesID.Compare)

	return slices.CompactFunc(s, func(a, b signal.SeriesID) bool { return a == b })
}

// All returns a Postings over every series in the index.
func (p *MemPostings) All() Postings {
	p.ensureSorted()

	return FromSlice(p.all)
}

// Get returns the series carrying exactly name=value.
func (p *MemPostings) Get(name, value []byte) Postings {
	p.ensureSorted()

	byVal := p.m[string(name)]
	if byVal == nil {
		return Empty()
	}

	return FromSlice(byVal[string(value)])
}

// ForName returns the series that carry the attribute name with any value.
func (p *MemPostings) ForName(name []byte) Postings {
	p.ensureSorted()

	byVal := p.m[string(name)]
	if len(byVal) == 0 {
		return Empty()
	}

	its := make([]Postings, 0, len(byVal))
	for _, ids := range byVal {
		its = append(its, FromSlice(ids))
	}

	return Merge(its...)
}

// WithoutName returns the series that do not carry the attribute name at all.
func (p *MemPostings) WithoutName(name []byte) Postings {
	return Without(p.All(), p.ForName(name))
}

// LabelValues returns, sorted, the distinct values seen for name.
func (p *MemPostings) LabelValues(name []byte) [][]byte {
	byVal := p.m[string(name)]
	if len(byVal) == 0 {
		return nil
	}

	out := make([][]byte, 0, len(byVal))
	for v := range byVal {
		out = append(out, []byte(v))
	}

	slices.SortFunc(out, slices.Compare)

	return out
}

// Select returns the union of the series whose value for name satisfies match — a
// caller-supplied predicate, so storage stays free of any query-language operator. Use
// it for regexp / range / custom conditions; for exact equality prefer [MemPostings.Get]
// (an O(1) lookup instead of a values scan).
//
// match must not retain or mutate value: the same buffer is reused across the scan.
func (p *MemPostings) Select(name []byte, match func(value []byte) bool) Postings {
	p.ensureSorted()

	byVal := p.m[string(name)]
	if len(byVal) == 0 {
		return Empty()
	}

	var (
		its     []Postings
		scratch []byte
	)

	for v, ids := range byVal {
		scratch = append(scratch[:0], v...)
		if match(scratch) {
			its = append(its, FromSlice(ids))
		}
	}

	if len(its) == 0 {
		return Empty()
	}

	return Merge(its...)
}

// Matcher is a single label condition: the values of Name whose bytes satisfy Match.
// The callback form keeps storage language-neutral — a language package supplies the
// predicate (a compiled regexp's Match, an exact compare, a numeric range, …).
type Matcher struct {
	Name  []byte
	Match func(value []byte) bool
}

// Resolve returns the series matching all matchers (their intersection). With no
// matchers it returns every series. Each matcher is resolved with [MemPostings.Select];
// negation and absent-label semantics are composed by the caller with [Without] /
// [MemPostings.WithoutName].
func (p *MemPostings) Resolve(ms ...Matcher) Postings {
	if len(ms) == 0 {
		return p.All()
	}

	its := make([]Postings, len(ms))
	for i, m := range ms {
		its[i] = p.Select(m.Name, m.Match)
	}

	return Intersect(its...)
}

// ensureSorted sorts and deduplicates every list (and the all-set) in place. It runs on
// the first read after a write; set-op iterators require sorted, deduplicated inputs.
func (p *MemPostings) ensureSorted() {
	if p.sorted {
		return
	}

	for _, byVal := range p.m {
		for v, ids := range byVal {
			byVal[v] = sortDedup(ids)
		}
	}

	p.all = sortDedup(p.all)
	p.sorted = true
}

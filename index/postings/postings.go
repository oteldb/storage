// Package postings is the inverted index: for each (name, value) attribute — identified
// by **interned symbol ids**, not strings — it keeps the sorted list of
// [signal.SeriesID]s that carry it, and composes those lists with lazy set-op iterators
// ([Intersect]/[Merge]/[Without]) to resolve label matchers to series.
//
// Keys are uint32 symbol ids (from index/symbols), so the index is zero-alloc and
// memory-compact, and it preserves attribute types: the value id is interned from the
// value's type-tagged encoding, so int 5, "5" and 5.0 are distinct buckets — not a
// stringified collapse. Matching is **callback-based and typed at the edges**:
// [MemPostings.Select] hands a predicate the candidate value id, and the caller decodes
// it to a [signal.Value] and applies any rule (regexp, numeric range, exact). Storage
// knows nothing about a query language's operators.
package postings

import (
	"slices"

	"github.com/oteldb/storage/signal"
)

// MemPostings is the in-memory inverted index: nameID → valueID → sorted series list,
// plus the set of all series. The zero value is not usable; create one with
// [NewMemPostings]. Not safe for concurrent use; callers own synchronization (the first
// read sorts the lists in place).
type MemPostings struct {
	m      map[uint32]map[uint32][]signal.SeriesID
	all    []signal.SeriesID
	sorted bool
}

// NewMemPostings returns an empty index.
func NewMemPostings() *MemPostings {
	return &MemPostings{m: make(map[uint32]map[uint32][]signal.SeriesID)}
}

// Add records that series carries the attribute nameID=valueID (both interned symbol
// ids). Re-adding the same triple is fine; duplicates are removed on the first read.
func (p *MemPostings) Add(series signal.SeriesID, nameID, valueID uint32) {
	byVal := p.m[nameID]
	if byVal == nil {
		byVal = make(map[uint32][]signal.SeriesID)
		p.m[nameID] = byVal
	}

	byVal[valueID] = append(byVal[valueID], series)
	p.all = append(p.all, series)
	p.sorted = false
}

// All returns a Postings over every series in the index.
func (p *MemPostings) All() Postings {
	p.ensureSorted()

	return FromSlice(p.all)
}

// Get returns the series carrying exactly nameID=valueID.
func (p *MemPostings) Get(nameID, valueID uint32) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if byVal == nil {
		return Empty()
	}

	return FromSlice(byVal[valueID])
}

// ForName returns the series that carry the attribute nameID with any value.
func (p *MemPostings) ForName(nameID uint32) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if len(byVal) == 0 {
		return Empty()
	}

	its := make([]Postings, 0, len(byVal))
	for _, ids := range byVal {
		its = append(its, FromSlice(ids))
	}

	return Merge(its...)
}

// WithoutName returns the series that do not carry the attribute nameID at all.
func (p *MemPostings) WithoutName(nameID uint32) Postings {
	return Without(p.All(), p.ForName(nameID))
}

// LabelValues returns, sorted, the distinct value ids seen for nameID.
func (p *MemPostings) LabelValues(nameID uint32) []uint32 {
	byVal := p.m[nameID]
	if len(byVal) == 0 {
		return nil
	}

	out := make([]uint32, 0, len(byVal))
	for valueID := range byVal {
		out = append(out, valueID)
	}

	slices.Sort(out)

	return out
}

// Select returns the union of the series whose value id for nameID satisfies match. The
// predicate receives the candidate value id; the caller resolves it to a typed
// [signal.Value] (via the symbol table) and applies any rule, so storage stays free of
// query-language operators. For exact equality prefer [MemPostings.Get] (an O(1) lookup).
func (p *MemPostings) Select(nameID uint32, match func(valueID uint32) bool) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if len(byVal) == 0 {
		return Empty()
	}

	var its []Postings
	for valueID, ids := range byVal {
		if match(valueID) {
			its = append(its, FromSlice(ids))
		}
	}

	if len(its) == 0 {
		return Empty()
	}

	return Merge(its...)
}

// Matcher is a single label condition: the value ids of NameID that satisfy the Match
// predicate. The callback form keeps storage language- and type-neutral — the caller
// decodes the value id to a typed value and supplies the rule.
type Matcher struct {
	NameID uint32
	Match  func(valueID uint32) bool
}

// Resolve returns the series matching all matchers (their intersection). With no
// matchers it returns every series. Negation and absent-label semantics are composed by
// the caller with [Without] / [MemPostings.WithoutName].
func (p *MemPostings) Resolve(ms ...Matcher) Postings {
	if len(ms) == 0 {
		return p.All()
	}

	its := make([]Postings, len(ms))
	for i, m := range ms {
		its[i] = p.Select(m.NameID, m.Match)
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
		for valueID, ids := range byVal {
			byVal[valueID] = sortDedup(ids)
		}
	}

	p.all = sortDedup(p.all)
	p.sorted = true
}

func sortDedup(s []signal.SeriesID) []signal.SeriesID {
	slices.SortFunc(s, signal.SeriesID.Compare)

	return slices.CompactFunc(s, func(a, b signal.SeriesID) bool { return a == b })
}

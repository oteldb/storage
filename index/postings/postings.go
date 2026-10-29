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
	"cmp"
	"slices"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// MemPostings is the in-memory inverted index: nameID → valueID → sorted series list,
// plus the set of all series. The zero value is not usable; create one with
// [NewMemPostings]. Not safe for concurrent use; callers own synchronization (the first
// read sorts the lists in place).
type MemPostings = memoryPostings[signal.SeriesID]

// Dense indexes engine-local ordinals while retaining the same iterator operations.
type Dense = memoryPostings[uint32]

type memoryPostings[T comparable] struct {
	m         map[uint32]map[uint32][]T
	all       []T
	compare   func(T, T) int
	fromSlice func([]T) Postings
	sorted    bool
	// bytes accumulates the index's owned memory (map entries and posting-list capacity) as it is
	// added, so [MemPostings.SizeBytes] does not walk every list. The index is identity state that
	// outlives every flush, so its size is metered.
	bytes int64
}

// Sizes of the index's owned structures. Posting lists are counted by capacity, which the in-place
// sort/dedup on first read shrinks the length of but never the capacity.
var (
	// A name entry costs its slot in p.m — whose value is one pointer word, since a map value *is*
	// a pointer to the runtime's map header — plus that header, allocated once per label name.
	nameEntryBytes  = memsize.MapEntry[uint32, map[uint32][]signal.SeriesID]() + memsize.MapBase
	valueEntryBytes = memsize.MapEntry[uint32, []signal.SeriesID]()
)

// NewMemPostings returns an empty index.
func NewMemPostings() *MemPostings {
	return &MemPostings{m: make(map[uint32]map[uint32][]signal.SeriesID), compare: signal.SeriesID.Compare, fromSlice: FromSlice}
}

// NewDense returns an empty index of local uint32 references.
func NewDense() *Dense {
	return &Dense{m: make(map[uint32]map[uint32][]uint32), compare: cmp.Compare[uint32], fromSlice: fromDenseSlice}
}

// SizeBytes returns the index's resident footprint: the name→value maps and every posting list's
// backing array (the all-set included). Symbol ids are the keys, so no label bytes are counted
// here — they belong to the symbol table.
func (p *memoryPostings[T]) SizeBytes() int64 { return p.bytes }

// Add records that series carries the attribute nameID=valueID (both interned symbol
// ids). Re-adding the same triple is fine; duplicates are removed on the first read.
func (p *memoryPostings[T]) Add(series T, nameID, valueID uint32) {
	byVal := p.m[nameID]
	if byVal == nil {
		byVal = make(map[uint32][]T)
		p.m[nameID] = byVal
		p.bytes += nameEntryBytes
	}

	// A bucket, once created, always holds at least one id, so a nil list means a new value —
	// checked without the second-result map probe the hot path would otherwise pay per Add.
	ids := byVal[valueID]
	if ids == nil {
		p.bytes += valueEntryBytes
	}

	grown := cap(ids)
	ids = append(ids, series)
	byVal[valueID] = ids
	p.bytes += int64(cap(ids)-grown) * memsize.Of[T]()

	p.addAll(series)
	p.sorted = false
}

// AddSeries registers series in the all-set without associating any attribute, so a series with no
// indexed labels (e.g. a log stream whose resource and scope carry no attributes) is still returned
// by [MemPostings.All] and by [MemPostings.Resolve] with no matchers. Idempotent: duplicates are
// removed on the first read.
func (p *memoryPostings[T]) AddSeries(series T) {
	p.addAll(series)
	p.sorted = false
}

// All returns a Postings over every series in the index.
func (p *memoryPostings[T]) All() Postings {
	p.ensureSorted()

	return p.fromSlice(p.all)
}

// Get returns the series carrying exactly nameID=valueID.
func (p *memoryPostings[T]) Get(nameID, valueID uint32) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if byVal == nil {
		return Empty()
	}

	return p.fromSlice(byVal[valueID])
}

// ForName returns the series that carry the attribute nameID with any value.
func (p *memoryPostings[T]) ForName(nameID uint32) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if len(byVal) == 0 {
		return Empty()
	}

	its := make([]Postings, 0, len(byVal))
	for _, ids := range byVal {
		its = append(its, p.fromSlice(ids))
	}

	return Merge(its...)
}

// WithoutName returns the series that do not carry the attribute nameID at all.
func (p *memoryPostings[T]) WithoutName(nameID uint32) Postings {
	return Without(p.All(), p.ForName(nameID))
}

// LabelValues returns, sorted, the distinct value ids seen for nameID.
func (p *memoryPostings[T]) LabelValues(nameID uint32) []uint32 {
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

// ForEachName calls fn once per indexed attribute name with its cardinality: distinctValues is
// the number of distinct value ids seen for the name, and totalSeries is the number of distinct
// series carrying the name (with any value). It sorts/deduplicates the index in place on first
// call (like any read), so a series added more than once for the same value is counted once; since
// a series carries one value per name, summing the deduplicated value buckets yields the distinct
// series count without a cross-bucket union. Iteration order is map order (nondeterministic); a
// caller wanting a ranking sorts the collected results. Not safe for concurrent use; the caller
// owns synchronization.
func (p *memoryPostings[T]) ForEachName(fn func(nameID uint32, distinctValues, totalSeries int)) {
	p.ensureSorted()

	for nameID, byVal := range p.m {
		total := 0
		for _, ids := range byVal {
			total += len(ids)
		}

		fn(nameID, len(byVal), total)
	}
}

// Select returns the union of the series whose value id for nameID satisfies match. The
// predicate receives the candidate value id; the caller resolves it to a typed
// [signal.Value] (via the symbol table) and applies any rule, so storage stays free of
// query-language operators. For exact equality prefer [MemPostings.Get] (an O(1) lookup).
func (p *memoryPostings[T]) Select(nameID uint32, match func(valueID uint32) bool) Postings {
	p.ensureSorted()

	byVal := p.m[nameID]
	if len(byVal) == 0 {
		return Empty()
	}

	var its []Postings
	for valueID, ids := range byVal {
		if match(valueID) {
			its = append(its, p.fromSlice(ids))
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
func (p *memoryPostings[T]) Resolve(ms ...Matcher) Postings {
	if len(ms) == 0 {
		return p.All()
	}

	its := make([]Postings, len(ms))
	for i, m := range ms {
		its[i] = p.Select(m.NameID, m.Match)
	}

	return Intersect(its...)
}

// Sorted reports whether the index is already sorted (no read will mutate it). A caller that
// owns synchronization checks this under a read lock to decide whether it must upgrade to an
// exclusive lock and call [MemPostings.EnsureSorted] before issuing concurrent reads.
func (p *memoryPostings[T]) Sorted() bool { return p.sorted }

// EnsureSorted sorts and deduplicates the index in place (idempotent; a no-op once sorted). It
// is the exported form of the read-triggered lazy sort: a caller holding the index's **exclusive**
// lock calls it after writes so that subsequent **concurrent** reads (which only inspect the
// already-sorted lists) never trigger the in-place mutation. See [MemPostings.Sorted].
func (p *memoryPostings[T]) EnsureSorted() { p.ensureSorted() }

// addAll appends to the all-set, accounting for the backing array's growth.
func (p *memoryPostings[T]) addAll(series T) {
	if n := len(p.all); n > 0 && p.all[n-1] == series {
		return
	}
	grown := cap(p.all)
	p.all = append(p.all, series)
	p.bytes += int64(cap(p.all)-grown) * memsize.Of[T]()
}

// ensureSorted sorts and deduplicates every list (and the all-set) in place. It runs on
// the first read after a write; set-op iterators require sorted, deduplicated inputs.
func (p *memoryPostings[T]) ensureSorted() {
	if p.sorted {
		return
	}

	for _, byVal := range p.m {
		for valueID, ids := range byVal {
			byVal[valueID] = sortDedup(ids, p.compare)
		}
	}

	p.all = sortDedup(p.all, p.compare)
	p.sorted = true
}

func sortDedup[T comparable](s []T, compare func(T, T) int) []T {
	slices.SortFunc(s, compare)
	return slices.Compact(s)
}

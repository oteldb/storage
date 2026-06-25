// Package postings is the inverted index: for each (name, value) attribute it keeps the
// sorted list of [signal.SeriesID]s that carry it, and composes those lists with lazy
// set-op iterators ([Intersect]/[Merge]/[Without]) to resolve label matchers to series.
//
// Matchers here are **literal set algebra** over the index (equal / not-equal / regexp /
// not-regexp), not a query language: a language package (PromQL, …) extracts conditions
// and composes these primitives with its own semantics. Storage stays language-neutral.
package postings

import (
	"regexp"
	"slices"

	"github.com/go-faster/errors"

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

// MatchOp is a label-matcher operator: literal set semantics over the index.
type MatchOp uint8

const (
	// MatchEqual selects series whose name value equals Value.
	MatchEqual MatchOp = iota
	// MatchNotEqual selects all series except those whose name value equals Value.
	MatchNotEqual
	// MatchRegexp selects series whose name value fully matches the Value regexp.
	MatchRegexp
	// MatchNotRegexp selects all series except those whose name value matches Value.
	MatchNotRegexp
)

// Matcher is a single literal label condition.
type Matcher struct {
	Op    MatchOp
	Name  []byte
	Value []byte // exact value, or RE2 pattern for the regexp ops
}

// Resolve returns the series matching all matchers (their intersection). With no
// matchers it returns every series. A regexp matcher with an invalid pattern errors.
func (p *MemPostings) Resolve(ms ...Matcher) (Postings, error) {
	if len(ms) == 0 {
		return p.All(), nil
	}

	its := make([]Postings, 0, len(ms))
	for _, m := range ms {
		it, err := p.resolveMatcher(m)
		if err != nil {
			return nil, err
		}

		its = append(its, it)
	}

	return Intersect(its...), nil
}

func (p *MemPostings) resolveMatcher(m Matcher) (Postings, error) {
	switch m.Op {
	case MatchEqual:
		return p.Get(m.Name, m.Value), nil
	case MatchNotEqual:
		return Without(p.All(), p.Get(m.Name, m.Value)), nil
	case MatchRegexp:
		return p.resolveRegexp(m.Name, m.Value)
	case MatchNotRegexp:
		pos, err := p.resolveRegexp(m.Name, m.Value)
		if err != nil {
			return nil, err
		}

		return Without(p.All(), pos), nil
	default:
		return nil, errors.Errorf("postings: unknown match op %d", m.Op)
	}
}

// resolveRegexp returns the union of the series whose name value fully matches pattern.
func (p *MemPostings) resolveRegexp(name, pattern []byte) (Postings, error) {
	re, err := regexp.Compile("^(?:" + string(pattern) + ")$") // fully anchored, like PromQL
	if err != nil {
		return nil, errors.Wrapf(err, "compile regexp %q", pattern)
	}

	p.ensureSorted()

	byVal := p.m[string(name)]

	var its []Postings

	for v, ids := range byVal {
		if re.MatchString(v) {
			its = append(its, FromSlice(ids))
		}
	}

	if len(its) == 0 {
		return Empty(), nil
	}

	return Merge(its...), nil
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

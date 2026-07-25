// Package sparsegram extracts content-defined, variable-length grams from a byte slice.
//
// It exists to answer one question the whole-token bloom cannot: an unanchored substring predicate
// whose literal holds no interior token (`|= "deadbeef"`) yields no safe tokens, so it prunes
// nothing. Grams here are chosen by a rule that depends only on the bytes inside the gram, which
// makes them safe to probe for a substring match without stripping the literal's edges.
//
// The rule. Weight every bigram of the input. A byte range is a gram when its two border bigrams
// both outweigh every bigram strictly inside it. Because that test reads only bytes within the
// range, a gram of a needle is also a gram of every value containing that needle, whatever
// surrounds it there — the property [Extractor.Grams] callers rely on and
// FuzzNeedleGramsAreHaystackGrams pins down.
//
// This is the scheme GitHub's code search calls "sparse grams" and ClickHouse ships as
// `sparseGrams`; see ARCHITECTURE.md for where it sits relative to index/bloom.
//
// The package is EXPERIMENTAL and not wired into the write or read path. It exists to measure
// whether a gram index earns its size before any on-disk format commits to one.
package sparsegram

import (
	"slices"
)

// Defaults for [Extractor]. MinLen is 3 because a gram needs two border bigrams and at least one
// byte between them; MaxLen bounds the tail of long, near-unique values (URLs, stack frames) that
// would otherwise each contribute a gram per prefix.
const (
	DefaultMinLen = 3
	DefaultMaxLen = 24
)

// Gram is a half-open byte range of the input.
type Gram struct {
	Start, End int32
}

// Len returns the gram's length in bytes.
func (g Gram) Len() int { return int(g.End - g.Start) }

// Extractor produces the grams of a value. It carries reusable scratch, so one Extractor walking
// many values allocates nothing after the first. The zero value uses [DefaultMinLen]/[DefaultMaxLen]
// and is ready to use. Not safe for concurrent use.
type Extractor struct {
	// MinLen and MaxLen bound the emitted gram length in bytes. Zero means the default.
	MinLen, MaxLen int

	weights []uint32 // weights[i] = weight of the bigram at i
	hull    []int32  // indices into weights, weights strictly decreasing (suffix maxima)
	scratch []Gram   // reused by AppendHashes
}

// Grams appends the grams of s to dst and returns the extended slice.
//
// Ranges are emitted in order of their right edge. A value shorter than MinLen yields none, which
// is why a query literal under 3 bytes cannot prune — the caller must fall back to a scan.
func (e *Extractor) Grams(dst []Gram, s []byte) []Gram {
	minLen, maxLen := e.bounds()

	if len(s) < minLen {
		return dst
	}

	e.weigh(s)
	e.hull = e.hull[:0]

	emit := func(l, r int32) []Gram {
		// The gram spans from the left border bigram's first byte to the right border bigram's
		// last, so its length is (r+2)-l.
		g := Gram{Start: l, End: r + 2}
		if n := g.Len(); n >= minLen && n <= maxLen {
			dst = append(dst, g)
		}

		return dst
	}

	for r := range int32(len(e.weights)) {
		w := e.weights[r]

		// Every hull entry the new bigram outweighs closes a gram: the hull holds suffix maxima,
		// so each popped left border already outweighs everything between it and r.
		for len(e.hull) > 0 && e.weights[e.hull[len(e.hull)-1]] < w {
			l := e.hull[len(e.hull)-1]
			e.hull = e.hull[:len(e.hull)-1]
			dst = emit(l, r)
		}

		// The surviving top outweighs r, which is the mirror case: it is a valid left border for a
		// gram ending at r.
		if len(e.hull) > 0 {
			dst = emit(e.hull[len(e.hull)-1], r)
		}

		// Equal weights would make "strictly greater than the interior" ambiguous; keeping only the
		// rightmost of a run makes the emitted set a function of the bytes alone.
		for len(e.hull) > 0 && e.weights[e.hull[len(e.hull)-1]] == w {
			e.hull = e.hull[:len(e.hull)-1]
		}

		e.hull = append(e.hull, r)
	}

	return dst
}

// AppendHashes appends one hash per gram of s to dst. It is the form a bloom wants: no gram bytes
// are materialized.
func (e *Extractor) AppendHashes(dst []uint64, s []byte) []uint64 {
	e.scratch = e.Grams(e.scratch[:0], s)
	for _, g := range e.scratch {
		dst = append(dst, HashGram(s[g.Start:g.End]))
	}

	return dst
}

func (e *Extractor) bounds() (int, int) {
	lo, hi := e.MinLen, e.MaxLen
	if lo <= 0 {
		lo = DefaultMinLen
	}

	if hi <= 0 {
		hi = DefaultMaxLen
	}

	if lo < 3 {
		// Two border bigrams overlap below 3 bytes, so the "interior" is empty and every range
		// would qualify. Clamp rather than reject: callers size grams, they don't validate them.
		lo = 3
	}

	if hi < lo {
		hi = lo
	}

	return lo, hi
}

// weigh fills e.weights with one weight per bigram of s.
//
// The weight is a finalizer over the bigram's two bytes. It only has to be well distributed — the
// gram set is whatever the weights imply, and any deterministic weighting yields a valid (if
// differently sized) set. Changing it changes the on-disk gram set, so it is part of the format.
func (e *Extractor) weigh(s []byte) {
	n := len(s) - 1

	if cap(e.weights) < n {
		e.weights = make([]uint32, n, n+n/2)
	}

	e.weights = e.weights[:n]

	for i := range n {
		h := uint32(s[i])<<8 | uint32(s[i+1])
		h *= 0x9E3779B1
		h ^= h >> 15
		h *= 0x85EBCA6B
		h ^= h >> 13
		e.weights[i] = h
	}
}

// HashGram hashes a gram's bytes. Exported so the query and build sides cannot drift.
func HashGram(b []byte) uint64 {
	// FNV-1a, then a finalizer: short inputs, and the bloom wants the bits well mixed.
	h := uint64(1469598103934665603)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}

	h ^= h >> 33
	h *= 0xFF51AFD7ED558CCD
	h ^= h >> 33

	return h
}

// Covering drops any gram of s wholly contained in a longer one, in place, and returns the kept
// prefix. The query side uses it: if a value's gram set holds the longer gram it holds the shorter
// covered ones too, so probing both only costs time.
func Covering(grams []Gram) []Gram {
	kept := grams[:0]

	for i, g := range grams {
		covered := false

		for j, other := range grams {
			if i == j {
				continue
			}

			if other.Start <= g.Start && other.End >= g.End && other.Len() > g.Len() {
				covered = true

				break
			}
		}

		if !covered {
			kept = append(kept, g)
		}
	}

	return dedupe(kept)
}

func dedupe(grams []Gram) []Gram {
	kept := grams[:0]

	for i, g := range grams {
		if !slices.Contains(grams[:i], g) {
			kept = append(kept, g)
		}
	}

	return kept
}

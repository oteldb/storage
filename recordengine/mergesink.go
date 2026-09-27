package recordengine

import (
	"bytes"
	"slices"

	"github.com/zeebo/xxh3"

	"github.com/oteldb/storage/index/bloom"
)

// columnSink takes a merged part's byte-column values as they stream into its writer and derives what
// the part's sidecars need from them: the column's bloom and, for the attributes column, the distinct
// record keys. Neither needs the part's rows held.
type columnSink struct {
	bloom *bloomAccum
	keys  recordKeySet
	// seen maps a value's hash to its token count, so a repeated value is neither tokenized nor
	// decoded again; see [bloomBuilder.markRows] for why a 64-bit hash is enough.
	seen map[uint64]int32
}

// newColumnSink returns the sink byte column k of schema needs, or nil when it feeds no sidecar.
func newColumnSink(schema *Schema, k int) *columnSink {
	var s columnSink

	if mode := schema.byteColumn(k).Bloom; mode != BloomNone {
		s.bloom = &bloomAccum{mode: mode}
	}

	if attrs, ok := schema.attrsByteCol(); ok && attrs == k {
		s.keys = make(recordKeySet)
	}

	if s.bloom == nil && s.keys == nil {
		return nil
	}

	s.seen = make(map[uint64]int32)

	return &s
}

func (s *columnSink) add(v []byte) {
	b := s.bloom
	if b != nil {
		b.bytes += int64(len(v))

		// An equality column is mostly distinct values, so a dedup pass would hash every row to skip
		// almost nothing: its tokens go straight to the exact pair set instead.
		if b.mode == BloomEquality {
			b.addEquality(v)
			b = nil
		}
	}

	if b == nil && s.keys == nil {
		return
	}

	h := xxh3.Hash(v)
	if n, ok := s.seen[h]; ok {
		if b != nil {
			b.occurrences += int(n)
		}

		return
	}

	var n int32
	if b != nil {
		n = b.addValue(v)
	}

	if s.keys != nil {
		s.keys.add(v)
	}

	if len(s.seen) < maxDedupRows {
		s.seen[h] = n
	}
}

// bloomAccum builds a column's bloom from values streamed to it, producing the filter
// [bloomBuilder.build] builds over the same rows: the same token set, sized from the same total
// bytes, occurrences and distinct estimate ([filterItems]). The filter cannot be sized until the
// last row, so the distinct tokens are kept as their probe hashes meanwhile — O(distinct tokens),
// not O(rows).
type bloomAccum struct {
	mode        BloomMode
	tok         tokenizer
	bytes       int64
	occurrences int
	distinct    bloom.Sketch
	pairs       hashPairs
}

func (a *bloomAccum) addEquality(v []byte) {
	if len(v) > 0 {
		a.occurrences++
		a.token(v)
	}
}

// addValue adds a value not seen before and returns its token count.
func (a *bloomAccum) addValue(v []byte) int32 {
	n := a.tok.count(a.mode, v)
	a.occurrences += n
	a.tok.tokens(a.mode, v, a.token)

	return int32(n)
}

func (a *bloomAccum) token(t []byte) {
	h1, h2 := bloom.Hashes(t)
	if a.pairs.add(h1, h2) {
		a.distinct.Add(t)
	}
}

func (a *bloomAccum) encode() []byte {
	n := filterItems(a.mode, a.bytes,
		func() int { return a.occurrences },
		a.distinct.Estimate)

	f := bloom.New(n, falsePositiveRate(a.mode))
	a.pairs.each(f.AddHashes)

	return f.Encode(nil)
}

// hashPairs is a set of [bloom.Hashes] pairs, open-addressed so a token costs its 16 bytes and the
// table's slack rather than a map entry's overhead. The all-zero pair marks an empty slot, so the
// set tracks it apart.
type hashPairs struct {
	slots []hashPair
	n     int
	zero  bool
}

type hashPair struct{ h1, h2 uint64 }

const minHashPairSlots = 1 << 10

// add inserts the pair, reporting whether it was new.
func (s *hashPairs) add(h1, h2 uint64) bool {
	if h1 == 0 && h2 == 0 {
		if s.zero {
			return false
		}

		s.zero = true

		return true
	}

	if 4*(s.n+1) > 3*len(s.slots) {
		s.grow()
	}

	if !s.insert(hashPair{h1, h2}) {
		return false
	}

	s.n++

	return true
}

func (s *hashPairs) insert(p hashPair) bool {
	mask := uint64(len(s.slots) - 1)

	for i := p.h2 & mask; ; i = (i + 1) & mask {
		switch q := &s.slots[i]; *q {
		case hashPair{}:
			*q = p

			return true
		case p:
			return false
		}
	}
}

func (s *hashPairs) grow() {
	old := s.slots
	s.slots = make([]hashPair, max(2*len(old), minHashPairSlots))

	for _, p := range old {
		if p != (hashPair{}) {
			s.insert(p)
		}
	}
}

func (s *hashPairs) each(fn func(h1, h2 uint64)) {
	if s.zero {
		fn(0, 0)
	}

	for _, p := range s.slots {
		if p != (hashPair{}) {
			fn(p.h1, p.h2)
		}
	}
}

// recordKeySet is the distinct record-attribute keys of the attribute blobs added to it.
type recordKeySet map[string]struct{}

func (s recordKeySet) add(blob []byte) {
	forEachAttrKey(blob, func(key []byte) {
		if _, ok := s[string(key)]; !ok {
			s[string(key)] = struct{}{}
		}
	})
}

// sorted returns the keys as owned copies, in order.
func (s recordKeySet) sorted() [][]byte {
	if len(s) == 0 {
		return nil
	}

	out := make([][]byte, 0, len(s))
	for key := range s {
		out = append(out, []byte(key))
	}

	slices.SortFunc(out, bytes.Compare)

	return out
}

package postings

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

var errBoom = errors.New("boom")

// errPostings is an empty iterator that reports a terminal error.
type errPostings struct{ err error }

func (errPostings) Next() bool                { return false }
func (errPostings) Seek(signal.SeriesID) bool { return false }
func (errPostings) At() signal.SeriesID       { return signal.SeriesID{} }
func (e errPostings) Err() error              { return e.err }

func TestErrPropagation(t *testing.T) {
	t.Parallel()

	boom := errBoom
	one := func() Postings { return FromSlice(sortedSet(1)) }

	_, err := ToSlice(Intersect(errPostings{boom}, one()))
	require.ErrorIs(t, err, boom)
	_, err = ToSlice(Merge(errPostings{boom}, one()))
	require.ErrorIs(t, err, boom)
	_, err = ToSlice(Without(one(), errPostings{boom}))
	require.ErrorIs(t, err, boom)
	_, err = ToSlice(Without(errPostings{boom}, one()))
	require.ErrorIs(t, err, boom)

	assert.Equal(t, signal.SeriesID{}, Empty().At())
}

// TestNestedIntersectSeek exercises intersectPostings.Seek by nesting one Intersect
// inside another (the outer's align calls the inner's Seek).
func TestNestedIntersectSeek(t *testing.T) {
	t.Parallel()

	a := FromSlice(sortedSet(1, 2, 3, 4, 5, 6))
	b := FromSlice(sortedSet(2, 4, 6, 8))
	c := FromSlice(sortedSet(4, 6, 10))
	// (a ∩ b) ∩ c = {2,4,6} ∩ {4,6,10} = {4,6}
	got := mustSlice(t, Intersect(FromSlice(sortedSet(4, 6, 10, 12)), Intersect(a, b, c)))
	assert.Equal(t, sortedSet(4, 6), got)
}

func sid(lo uint64) signal.SeriesID { return signal.SeriesID{Lo: lo} }

func sortedSet(ids ...uint64) []signal.SeriesID {
	out := make([]signal.SeriesID, len(ids))
	for i, v := range ids {
		out[i] = sid(v)
	}

	slices.SortFunc(out, signal.SeriesID.Compare)

	return slices.CompactFunc(out, func(a, b signal.SeriesID) bool { return a == b })
}

func mustSlice(t *testing.T, p Postings) []signal.SeriesID {
	t.Helper()
	got, err := ToSlice(p)
	require.NoError(t, err)

	return got
}

func TestFromSliceAndSeek(t *testing.T) {
	t.Parallel()

	s := sortedSet(2, 4, 6, 8, 10)

	p := FromSlice(s)
	require.True(t, p.Seek(sid(5)))
	assert.Equal(t, sid(6), p.At())
	require.True(t, p.Seek(sid(6)), "seek to current stays")
	assert.Equal(t, sid(6), p.At())
	require.True(t, p.Next())
	assert.Equal(t, sid(8), p.At())
	assert.False(t, p.Seek(sid(99)), "seek past end")

	assert.Empty(t, mustSlice(t, Empty()))
	assert.False(t, Empty().Seek(sid(1)))
}

func TestIntersectBasic(t *testing.T) {
	t.Parallel()

	a := FromSlice(sortedSet(1, 2, 3, 4, 5))
	b := FromSlice(sortedSet(2, 4, 6))
	c := FromSlice(sortedSet(2, 4, 5))
	assert.Equal(t, sortedSet(2, 4), mustSlice(t, Intersect(a, b, c)))

	assert.Empty(t, mustSlice(t, Intersect(FromSlice(sortedSet(1)), Empty())))
	assert.Equal(t, sortedSet(1, 2), mustSlice(t, Intersect(FromSlice(sortedSet(1, 2)))))
}

func TestMergeBasic(t *testing.T) {
	t.Parallel()

	a := FromSlice(sortedSet(1, 3, 5))
	b := FromSlice(sortedSet(2, 3, 4))
	assert.Equal(t, sortedSet(1, 2, 3, 4, 5), mustSlice(t, Merge(a, b)))
}

// TestMergeManyBucketsVsNaive exercises the heap-based union at large k (many input buckets), where
// the heapify / siftDown / removeRoot paths actually run — the small-k property test (k≤3) does not.
// It checks a full drain and a battery of Seeks against the naive union reference.
func TestMergeManyBucketsVsNaive(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(7, 11))

	for range 200 {
		k := 2 + rng.IntN(64) // up to 65 input buckets

		sets := make([][]signal.SeriesID, k)
		its := make([]Postings, k)

		for i := range sets {
			sets[i] = randSet(rng, 400)
			its[i] = FromSlice(sets[i])
		}

		want := naiveUnion(sets...)

		// Full drain matches the naive union.
		assert.Equal(t, orNil(want), mustSlice(t, Merge(its...)))

		// Seek to several targets matches the first union id ≥ target (heap rebuild path).
		for range 10 {
			target := signal.SeriesID{Hi: uint64(rng.IntN(2)), Lo: uint64(rng.IntN(400))}

			freshIts := make([]Postings, k)
			for i := range sets {
				freshIts[i] = FromSlice(sets[i])
			}

			m := Merge(freshIts...)
			gotOK := m.Seek(target)

			var wantID signal.SeriesID

			wantOK := false

			for _, id := range want {
				if !id.Less(target) {
					wantID, wantOK = id, true

					break
				}
			}

			require.Equal(t, wantOK, gotOK, "seek presence for target %v", target)

			if wantOK {
				assert.Equal(t, wantID, m.At(), "seek landed on the wrong id for target %v", target)
			}
		}
	}
}

func TestWithoutBasic(t *testing.T) {
	t.Parallel()

	a := FromSlice(sortedSet(1, 2, 3, 4, 5))
	b := FromSlice(sortedSet(2, 4))
	assert.Equal(t, sortedSet(1, 3, 5), mustSlice(t, Without(a, b)))

	// Removing everything, and removing nothing.
	assert.Empty(t, mustSlice(t, Without(FromSlice(sortedSet(1, 2)), FromSlice(sortedSet(1, 2)))))
	assert.Equal(t, sortedSet(1, 2), mustSlice(t, Without(FromSlice(sortedSet(1, 2)), Empty())))
}

// Naive reference set operations over slices for the property tests.
func naiveInter(sets ...[]signal.SeriesID) []signal.SeriesID {
	if len(sets) == 0 {
		return nil
	}

	var out []signal.SeriesID
	for _, id := range sets[0] {
		inAll := true
		for _, s := range sets[1:] {
			if !slices.Contains(s, id) {
				inAll = false

				break
			}
		}

		if inAll {
			out = append(out, id)
		}
	}

	return out
}

func naiveUnion(sets ...[]signal.SeriesID) []signal.SeriesID {
	seen := map[signal.SeriesID]struct{}{}
	for _, s := range sets {
		for _, id := range s {
			seen[id] = struct{}{}
		}
	}

	out := make([]signal.SeriesID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}

	slices.SortFunc(out, signal.SeriesID.Compare)

	return out
}

func naiveWithout(a, b []signal.SeriesID) []signal.SeriesID {
	var out []signal.SeriesID
	for _, id := range a {
		if !slices.Contains(b, id) {
			out = append(out, id)
		}
	}

	return out
}

func randSet(rng *rand.Rand, maxVal int) []signal.SeriesID {
	n := rng.IntN(maxVal + 1)

	seen := map[signal.SeriesID]struct{}{}
	for range n {
		seen[signal.SeriesID{Hi: uint64(rng.IntN(2)), Lo: uint64(rng.IntN(maxVal))}] = struct{}{}
	}

	out := make([]signal.SeriesID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}

	slices.SortFunc(out, signal.SeriesID.Compare)

	return out
}

// TestSetOpsVsNaive fuzzes Intersect/Merge/Without (including nested compositions)
// against the naive reference.
func TestSetOpsVsNaive(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))
	for range 3000 {
		a, b, c, d := randSet(rng, 20), randSet(rng, 20), randSet(rng, 20), randSet(rng, 20)

		assert.Equal(t, orNil(naiveInter(a, b, c)), mustSlice(t, Intersect(FromSlice(a), FromSlice(b), FromSlice(c))))
		assert.Equal(t, naiveUnion(a, b, c), mustSlice(t, Merge(FromSlice(a), FromSlice(b), FromSlice(c))))
		assert.Equal(t, orNil(naiveWithout(a, b)), mustSlice(t, Without(FromSlice(a), FromSlice(b))))

		// Nested: (a ∪ b) ∩ (c \ d) — exercises Seek across composed iterators.
		want := naiveInter(naiveUnion(a, b), naiveWithout(c, d))
		got := mustSlice(t, Intersect(Merge(FromSlice(a), FromSlice(b)), Without(FromSlice(c), FromSlice(d))))
		assert.Equal(t, orNil(want), got)
	}
}

// orNil normalizes an empty result to nil (ToSlice yields nil for an empty iterator).
func orNil(s []signal.SeriesID) []signal.SeriesID {
	if len(s) == 0 {
		return nil
	}

	return s
}

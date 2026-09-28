package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestIntervalValid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		iv   bucketindex.Interval
		want bool
	}{
		{"zero", bucketindex.Interval{}, false},
		{"block zero", bucketindex.Range(0, 0, 5), false},
		{"max zero", bucketindex.Range(0, 5, 0), false},
		{"inverted", bucketindex.Range(0, 9, 3), false},
		{"single", bucketindex.Range(0, 1, 1), true},
		{"range", bucketindex.Range(0, 3, 9), true},
		{"term range", bucketindex.Range(7, 3, 9), true},
		{"run across terms", bucketindex.Interval{Min: bucketindex.Block{Term: 1, N: 5}, Max: bucketindex.Block{Term: 2, N: 3}}, false},
		{"run from number zero", bucketindex.Range(4, 0, 3), false},
		{"two terms", bucketindex.TermBlocks(1, 5).Union(bucketindex.TermBlocks(2, 1)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.iv.Valid())
		})
	}
}

func TestIntervalEqual(t *testing.T) {
	t.Parallel()

	gapped := bucketindex.Blocks(1, 2, 5, 6)

	cases := []struct {
		name string
		a, b bucketindex.Interval
		want bool
	}{
		{"unset", bucketindex.Interval{}, bucketindex.Interval{}, true},
		{"single", bucketindex.Blocks(3), bucketindex.Blocks(3), true},
		{"gapped", gapped, bucketindex.Blocks(6, 5, 2, 1), true},
		{"nil and empty gaps", bucketindex.Range(0, 1, 2), bucketindex.Interval{Min: bucketindex.Block{N: 1}, Max: bucketindex.Block{N: 2}, Gaps: []bucketindex.Gap{}}, true},
		{"min", bucketindex.Blocks(3), bucketindex.Blocks(4), false},
		{"max", bucketindex.Blocks(3, 4), bucketindex.Blocks(3, 4, 5), false},
		{"gap", gapped, bucketindex.Blocks(1, 2, 3, 5, 6), false},
		{"unset and set", bucketindex.Interval{}, bucketindex.Blocks(1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.a.Equal(tc.b))
			assert.Equal(t, tc.want, tc.b.Equal(tc.a))
		})
	}
}

func TestClaimEqual(t *testing.T) {
	t.Parallel()

	claim := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2, 3), Group: bucketindex.Blocks(7, 8)}

	assert.True(t, claim.Equal(bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2, 3), Group: bucketindex.Blocks(7, 8)}))
	assert.True(t, bucketindex.Claim{}.Equal(bucketindex.Claim{}))
	assert.False(t, claim.Equal(bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(7, 8)}))
	assert.False(t, claim.Equal(bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2, 3), Group: bucketindex.Blocks(7, 9)}))
	assert.False(t, claim.Equal(bucketindex.Claim{}))
}

// TestIntervalZeroContainsNothing pins the dangerous failure mode: a pre-v5 entry carries no
// interval, and its zero value must neither contain nor be contained by anything.
func TestIntervalZeroContainsNothing(t *testing.T) {
	t.Parallel()

	var unset bucketindex.Interval
	set := bucketindex.Range(0, 1, 10)

	assert.False(t, unset.Contains(set))
	assert.False(t, set.Contains(unset))
	assert.False(t, unset.Contains(unset))
	assert.Zero(t, unset.Len())
}

func TestIntervalContains(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		outer    bucketindex.Interval
		inner    bucketindex.Interval
		contains bool
	}{
		{"identical", bucketindex.Range(0, 3, 3), bucketindex.Range(0, 3, 3), true},
		{"strict", bucketindex.Range(0, 1, 10), bucketindex.Range(0, 4, 6), true},
		{"left edge", bucketindex.Range(0, 1, 10), bucketindex.Range(0, 1, 2), true},
		{"right edge", bucketindex.Range(0, 1, 10), bucketindex.Range(0, 9, 10), true},
		{"overhang", bucketindex.Range(0, 2, 10), bucketindex.Range(0, 1, 10), false},
		{"disjoint", bucketindex.Range(0, 1, 2), bucketindex.Range(0, 3, 4), false},
		{"inverted outer", bucketindex.Range(0, 10, 1), bucketindex.Range(0, 3, 4), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.contains, tc.outer.Contains(tc.inner))
		})
	}
}

func TestIntervalUnion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		a, b, ab bucketindex.Interval
	}{
		{"adjacent", bucketindex.Range(0, 1, 1), bucketindex.Range(0, 2, 2), bucketindex.Range(0, 1, 2)},
		// The union is a set, not a hull: blocks 2 to 4 belong to neither side and the result must
		// not claim them, or a merge of a lost part's neighbors would discharge the want for it.
		{
			"gapped",
			bucketindex.Range(0, 1, 1), bucketindex.Range(0, 5, 7),
			bucketindex.Interval{Min: bucketindex.Block{N: 1}, Max: bucketindex.Block{N: 7}, Gaps: []bucketindex.Gap{{Min: bucketindex.Block{N: 2}, Max: bucketindex.Block{N: 4}}}},
		},
		{"nested", bucketindex.Range(0, 1, 9), bucketindex.Range(0, 4, 5), bucketindex.Range(0, 1, 9)},
		{"unset right", bucketindex.Range(0, 2, 3), bucketindex.Interval{}, bucketindex.Range(0, 2, 3)},
		{"unset left", bucketindex.Interval{}, bucketindex.Range(0, 2, 3), bucketindex.Range(0, 2, 3)},
		{"both unset", bucketindex.Interval{}, bucketindex.Interval{}, bucketindex.Interval{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.ab, tc.a.Union(tc.b))
			assert.Equal(t, tc.ab, tc.b.Union(tc.a), "union is symmetric")
		})
	}
}

func TestIntervalBlocks(t *testing.T) {
	t.Parallel()

	assert.EqualValues(t, 1, bucketindex.Range(0, 7, 7).Len())
	assert.EqualValues(t, 8, bucketindex.Range(0, 3, 10).Len())
}

// TestIntervalAcrossTerms pins the set a merge across tenures holds: each term's blocks, and a gap
// spanning the rest of the lower term's number space and the start of the higher one's.
func TestIntervalAcrossTerms(t *testing.T) {
	t.Parallel()

	iv := bucketindex.TermBlocks(1, 5, 6).Union(bucketindex.TermBlocks(2, 1))
	require.True(t, iv.Valid())

	assert.Equal(t, bucketindex.Interval{
		Min: bucketindex.Block{Term: 1, N: 5},
		Max: bucketindex.Block{Term: 2, N: 1},
		Gaps: []bucketindex.Gap{{
			Min: bucketindex.Block{Term: 1, N: 7},
			Max: bucketindex.Block{Term: 2, N: 0},
		}},
	}, iv)
	assert.EqualValues(t, 3, iv.Len())
	assert.True(t, iv.Contains(bucketindex.TermBlocks(2, 1)))
	assert.False(t, iv.Contains(bucketindex.TermBlocks(1, 7)), "the rest of term 1 is not held")
	assert.False(t, iv.Contains(bucketindex.Blocks(5)), "nor is term 0's block of the same number")

	var seen []bucketindex.Block

	iv.Each(func(b bucketindex.Block) bool {
		seen = append(seen, b)

		return len(seen) < 2
	})
	assert.Equal(t, []bucketindex.Block{{Term: 1, N: 5}, {Term: 1, N: 6}}, seen, "Each stops when told to")
}

func TestSupersedes(t *testing.T) {
	t.Parallel()

	flush := func(n uint64) bucketindex.Entry {
		return bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Range(0, n, n)}
	}
	merged := bucketindex.Entry{Prefix: "m", Blocks: bucketindex.Range(0, 1, 3), Level: 1}

	assert.True(t, merged.Supersedes(flush(1)))
	assert.True(t, merged.Supersedes(flush(3)))
	assert.False(t, merged.Supersedes(flush(4)), "outside the interval")
	assert.False(t, flush(1).Supersedes(merged), "narrower and lower")
	assert.False(t, merged.Supersedes(merged), "same level does not supersede itself")

	sameLevel := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Range(0, 1, 5)}
	assert.False(t, sameLevel.Supersedes(flush(2)), "containment alone is not supersession")
}

// TestSupersedesPreV5 pins the documented fallback: an entry written before v5 carries no interval,
// so it takes part in no supersession in either direction and wants naming it match by prefix only.
func TestSupersedesPreV5(t *testing.T) {
	t.Parallel()

	old := bucketindex.Entry{Prefix: "old"}
	merged := bucketindex.Entry{Prefix: "m", Blocks: bucketindex.Range(0, 1, 100), Level: 3}

	assert.False(t, merged.Supersedes(old))
	assert.False(t, old.Supersedes(merged))
	assert.False(t, old.Supersedes(bucketindex.Entry{Prefix: "other"}))
}

func TestNextBlock(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	assert.Equal(t, bucketindex.Block{N: 1}, ix.NextBlock(0), "block numbering starts at 1")

	ix.Add(bucketindex.Entry{Prefix: "a"})
	assert.Equal(t, bucketindex.Block{N: 1}, ix.NextBlock(0), "a pre-v5 entry claims no block")

	ix.Add(bucketindex.Entry{Prefix: "b", Blocks: bucketindex.Range(0, 1, 4), Level: 1})
	ix.Add(bucketindex.Entry{Prefix: "c", Blocks: bucketindex.Range(0, 5, 5)})
	assert.Equal(t, bucketindex.Block{N: 6}, ix.NextBlock(0))

	// A want's blocks stay claimed: the part may yet be repaired back in.
	ix.RecordWant(bucketindex.Want{Prefix: "d", Blocks: bucketindex.Range(0, 9, 9)})
	assert.Equal(t, bucketindex.Block{N: 10}, ix.NextBlock(0))
}

func TestSatisfying(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	ix.Add(bucketindex.Entry{Prefix: "small", Blocks: bucketindex.Range(0, 1, 2), Level: 1})
	ix.Add(bucketindex.Entry{Prefix: "big", Blocks: bucketindex.Range(0, 1, 8), Level: 2})

	got, ok := ix.Satisfying(bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Range(0, 2, 2)})
	require.True(t, ok)
	assert.Equal(t, "big", got.Prefix, "the largest containing part wins")

	_, ok = ix.Satisfying(bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Range(0, 9, 9)})
	assert.False(t, ok, "no part covers the block")

	// An exact prefix hit satisfies even when the entry carries no interval.
	ix.Add(bucketindex.Entry{Prefix: "old"})
	got, ok = ix.Satisfying(bucketindex.Want{Prefix: "old"})
	require.True(t, ok)
	assert.Equal(t, "old", got.Prefix)

	_, ok = ix.Satisfying(bucketindex.Want{Prefix: "lost-pre-v5"})
	assert.False(t, ok, "a want with no interval matches by prefix only")
}

func TestSatisfyingTieBreak(t *testing.T) {
	t.Parallel()

	w := bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Range(0, 2, 2)}

	byLevel := &bucketindex.Index{}
	byLevel.Add(bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Range(0, 1, 4), Level: 1})
	byLevel.Add(bucketindex.Entry{Prefix: "a", Blocks: bucketindex.Range(0, 1, 4), Level: 2})

	got, ok := byLevel.Satisfying(w)
	require.True(t, ok)
	assert.Equal(t, "a", got.Prefix, "equal width, higher level")

	byPrefix := &bucketindex.Index{}
	byPrefix.Add(bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Range(0, 1, 4), Level: 1})
	byPrefix.Add(bucketindex.Entry{Prefix: "a", Blocks: bucketindex.Range(0, 1, 4), Level: 1})

	got, ok = byPrefix.Satisfying(w)
	require.True(t, ok)
	assert.Equal(t, "a", got.Prefix, "equal width and level, lowest prefix, so the answer is stable")
}

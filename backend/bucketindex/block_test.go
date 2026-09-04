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
		{"block zero", bucketindex.Interval{Min: 0, Max: 5}, false},
		{"max zero", bucketindex.Interval{Min: 5, Max: 0}, false},
		{"inverted", bucketindex.Interval{Min: 9, Max: 3}, false},
		{"single", bucketindex.Interval{Min: 1, Max: 1}, true},
		{"range", bucketindex.Interval{Min: 3, Max: 9}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.iv.Valid())
		})
	}
}

// TestIntervalZeroContainsNothing pins the dangerous failure mode: a pre-v5 entry carries no
// interval, and its zero value must neither contain nor be contained by anything.
func TestIntervalZeroContainsNothing(t *testing.T) {
	t.Parallel()

	var unset bucketindex.Interval
	set := bucketindex.Interval{Min: 1, Max: 10}

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
		{"identical", bucketindex.Interval{Min: 3, Max: 3}, bucketindex.Interval{Min: 3, Max: 3}, true},
		{"strict", bucketindex.Interval{Min: 1, Max: 10}, bucketindex.Interval{Min: 4, Max: 6}, true},
		{"left edge", bucketindex.Interval{Min: 1, Max: 10}, bucketindex.Interval{Min: 1, Max: 2}, true},
		{"right edge", bucketindex.Interval{Min: 1, Max: 10}, bucketindex.Interval{Min: 9, Max: 10}, true},
		{"overhang", bucketindex.Interval{Min: 2, Max: 10}, bucketindex.Interval{Min: 1, Max: 10}, false},
		{"disjoint", bucketindex.Interval{Min: 1, Max: 2}, bucketindex.Interval{Min: 3, Max: 4}, false},
		{"inverted outer", bucketindex.Interval{Min: 10, Max: 1}, bucketindex.Interval{Min: 3, Max: 4}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.contains, tc.outer.Contains(tc.inner))
		})
	}
}

func TestIntervalBlocks(t *testing.T) {
	t.Parallel()

	assert.EqualValues(t, 1, bucketindex.Interval{Min: 7, Max: 7}.Len())
	assert.EqualValues(t, 8, bucketindex.Interval{Min: 3, Max: 10}.Len())
}

func TestSupersedes(t *testing.T) {
	t.Parallel()

	flush := func(n uint64) bucketindex.Entry {
		return bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Interval{Min: n, Max: n}}
	}
	merged := bucketindex.Entry{Prefix: "m", Blocks: bucketindex.Interval{Min: 1, Max: 3}, Level: 1}

	assert.True(t, merged.Supersedes(flush(1)))
	assert.True(t, merged.Supersedes(flush(3)))
	assert.False(t, merged.Supersedes(flush(4)), "outside the interval")
	assert.False(t, flush(1).Supersedes(merged), "narrower and lower")
	assert.False(t, merged.Supersedes(merged), "same level does not supersede itself")

	sameLevel := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Interval{Min: 1, Max: 5}}
	assert.False(t, sameLevel.Supersedes(flush(2)), "containment alone is not supersession")
}

// TestSupersedesPreV5 pins the documented fallback: an entry written before v5 carries no interval,
// so it takes part in no supersession in either direction and wants naming it match by prefix only.
func TestSupersedesPreV5(t *testing.T) {
	t.Parallel()

	old := bucketindex.Entry{Prefix: "old"}
	merged := bucketindex.Entry{Prefix: "m", Blocks: bucketindex.Interval{Min: 1, Max: 100}, Level: 3}

	assert.False(t, merged.Supersedes(old))
	assert.False(t, old.Supersedes(merged))
	assert.False(t, old.Supersedes(bucketindex.Entry{Prefix: "other"}))
}

func TestNextBlock(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	assert.EqualValues(t, 1, ix.NextBlock(), "block numbering starts at 1")

	ix.Add(bucketindex.Entry{Prefix: "a"})
	assert.EqualValues(t, 1, ix.NextBlock(), "a pre-v5 entry claims no block")

	ix.Add(bucketindex.Entry{Prefix: "b", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 1})
	ix.Add(bucketindex.Entry{Prefix: "c", Blocks: bucketindex.Interval{Min: 5, Max: 5}})
	assert.EqualValues(t, 6, ix.NextBlock())

	// A want's blocks stay claimed: the part may yet be repaired back in.
	ix.RecordWant(bucketindex.Want{Prefix: "d", Blocks: bucketindex.Interval{Min: 9, Max: 9}})
	assert.EqualValues(t, 10, ix.NextBlock())
}

func TestSatisfying(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	ix.Add(bucketindex.Entry{Prefix: "small", Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1})
	ix.Add(bucketindex.Entry{Prefix: "big", Blocks: bucketindex.Interval{Min: 1, Max: 8}, Level: 2})

	got, ok := ix.Satisfying(bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	require.True(t, ok)
	assert.Equal(t, "big", got.Prefix, "the largest containing part wins")

	_, ok = ix.Satisfying(bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Interval{Min: 9, Max: 9}})
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

	w := bucketindex.Want{Prefix: "gone", Blocks: bucketindex.Interval{Min: 2, Max: 2}}

	byLevel := &bucketindex.Index{}
	byLevel.Add(bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 1})
	byLevel.Add(bucketindex.Entry{Prefix: "a", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2})

	got, ok := byLevel.Satisfying(w)
	require.True(t, ok)
	assert.Equal(t, "a", got.Prefix, "equal width, higher level")

	byPrefix := &bucketindex.Index{}
	byPrefix.Add(bucketindex.Entry{Prefix: "z", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 1})
	byPrefix.Add(bucketindex.Entry{Prefix: "a", Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 1})

	got, ok = byPrefix.Satisfying(w)
	require.True(t, ok)
	assert.Equal(t, "a", got.Prefix, "equal width and level, lowest prefix, so the answer is stable")
}

package bucketindex_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

// members is the block set as a plain sorted list — the naive reference the algebra is checked
// against, so a bug in the runs-and-gaps encoding cannot hide behind the same bug in the test.
func members(iv bucketindex.Interval) []uint64 {
	var out []uint64

	iv.Each(func(b uint64) bool {
		out = append(out, b)

		return true
	})

	return out
}

func randomSet(rnd *rand.Rand, span uint64) ([]uint64, bucketindex.Interval) {
	var nums []uint64

	for b := uint64(1); b <= span; b++ {
		if rnd.IntN(3) == 0 {
			nums = append(nums, b)
		}
	}

	return nums, bucketindex.Blocks(nums...)
}

func TestBlocksMatchesTheNaiveSet(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(7, 11))
	for range 500 {
		nums, iv := randomSet(rnd, 12)

		require.Equal(t, len(nums) > 0, iv.Valid())
		assert.Equal(t, nums, members(iv))
		assert.EqualValues(t, len(nums), iv.Len())
	}
}

// TestContainsMatchesSubset checks containment against a brute-force subset test.
func TestContainsMatchesSubset(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(13, 17))
	for range 2000 {
		an, a := randomSet(rnd, 10)
		bn, b := randomSet(rnd, 10)

		subset := len(bn) > 0 && len(an) > 0
		for _, n := range bn {
			if !slices.Contains(an, n) {
				subset = false

				break
			}
		}

		assert.Equalf(t, subset, a.Contains(b), "%v contains %v", an, bn)
	}
}

// TestUnionMatchesSetUnion is the law #559 turns on: the union claims what its operands hold and
// not one block more, so merging around a gap never covers it.
func TestUnionMatchesSetUnion(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(19, 23))
	for range 2000 {
		an, a := randomSet(rnd, 10)
		bn, b := randomSet(rnd, 10)

		want := slices.Compact(slices.Sorted(slices.Values(slices.Concat(an, bn))))

		ab := a.Union(b)
		assert.Equal(t, want, members(ab))
		assert.Equal(t, ab, b.Union(a), "union is symmetric")
		assert.Equal(t, ab, ab.Union(ab), "union is idempotent")
		// Only over set operands: an unset interval takes part in no containment, by design, so it
		// is neither inside the union nor an argument against it.
		if a.Valid() && b.Valid() {
			assert.True(t, ab.Contains(a) && ab.Contains(b), "union contains both")
		}
	}
}

func TestUnionIsAssociative(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(29, 31))
	for range 1000 {
		_, a := randomSet(rnd, 8)
		_, b := randomSet(rnd, 8)
		_, c := randomSet(rnd, 8)

		assert.Equal(t, a.Union(b).Union(c), a.Union(b.Union(c)))
	}
}

// TestSupersessionLaws pins the relation's algebra: it is irreflexive (level is strict), transitive,
// and never true of an unset identity.
func TestSupersessionLaws(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(37, 41))
	entry := func() bucketindex.Entry {
		_, iv := randomSet(rnd, 8)

		return bucketindex.Entry{Blocks: iv, Level: uint32(rnd.IntN(4))}
	}

	for range 3000 {
		a, b, c := entry(), entry(), entry()

		assert.False(t, a.Supersedes(a), "a part never supersedes itself: the level must rise")

		if a.Supersedes(b) && b.Supersedes(c) {
			assert.True(t, a.Supersedes(c), "supersession is transitive")
		}

		if a.Supersedes(b) {
			assert.False(t, b.Supersedes(a), "and antisymmetric")
			assert.True(t, a.Blocks.Contains(b.Blocks))
		}

		unset := bucketindex.Entry{Level: a.Level + 1}
		assert.False(t, unset.Supersedes(a), "an unset identity claims nothing")
		assert.False(t, a.Supersedes(unset), "and is claimed by nothing")
	}
}

// TestNoHullClaimsAnUnheldBlock is #559 as a law: a merge output's identity is the union of its
// inputs', so no block outside the inputs is ever claimed however the run straddles gaps.
func TestNoHullClaimsAnUnheldBlock(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(43, 47))
	for range 1000 {
		inputs := make([]bucketindex.Interval, 0, 4)
		held := map[uint64]struct{}{}

		var union bucketindex.Interval

		for range rnd.IntN(4) + 1 {
			nums, iv := randomSet(rnd, 9)
			for _, n := range nums {
				held[n] = struct{}{}
			}

			inputs = append(inputs, iv)
			union = union.Union(iv)
		}

		for _, b := range members(union) {
			_, ok := held[b]
			assert.Truef(t, ok, "block %d is claimed by the union of %v but held by no input", b, inputs)
		}
	}
}

// TestJointCoverageNeedsEveryMember pins the split-group rule: the ancestry a group claims counts as
// present only once every one of its members is, and one fragment short of the set claims nothing.
func TestJointCoverageNeedsEveryMember(t *testing.T) {
	t.Parallel()

	claim := bucketindex.Claim{
		Blocks: bucketindex.Blocks(1, 2, 3),
		Group:  bucketindex.Blocks(4, 5, 6),
	}
	frag := func(prefix string, b uint64) bucketindex.Entry {
		return bucketindex.Entry{Prefix: prefix, Blocks: bucketindex.Blocks(b), Claim: claim, Level: 1}
	}

	want := bucketindex.Want{Prefix: "lost", Blocks: bucketindex.Blocks(2), Level: 0}

	partial := &bucketindex.Index{Entries: []bucketindex.Entry{frag("a", 4), frag("b", 5)}}
	_, ok := partial.Satisfying(want)
	assert.False(t, ok, "two thirds of a group answers nothing")
	assert.Equal(t, []uint64{6}, partial.Missing(want), "and the third is what repair must fetch")

	whole := &bucketindex.Index{Entries: []bucketindex.Entry{frag("a", 4), frag("b", 5), frag("c", 6)}}
	got, ok := whole.Satisfying(want)
	require.True(t, ok, "the complete group holds every row the want names")
	assert.False(t, got.Supersedes(want.Entry()), "though no member of it contains the want alone")
	assert.Empty(t, whole.Missing(want))

	// A hole is not data, so it can never stand in for a missing member.
	hole := frag("c", 6)
	hole.Hole = true
	holed := &bucketindex.Index{Entries: []bucketindex.Entry{frag("a", 4), frag("b", 5), hole}}
	_, ok = holed.Satisfying(want)
	assert.False(t, ok, "an acknowledged loss completes no group")
}

// TestSubsumedRetiresWhatACompletedGroupCovers pins that finishing a split group retires the
// ancestors it covers: leaving them live beside it would read their rows twice.
func TestSubsumedRetiresWhatACompletedGroupCovers(t *testing.T) {
	t.Parallel()

	claim := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(3, 4)}
	frag := func(prefix string, b uint64) bucketindex.Entry {
		return bucketindex.Entry{Prefix: prefix, Blocks: bucketindex.Blocks(b), Claim: claim, Level: 1}
	}

	ancestor := bucketindex.Entry{Prefix: "old", Blocks: bucketindex.Blocks(1), Level: 0}
	bystander := bucketindex.Entry{Prefix: "by", Blocks: bucketindex.Blocks(9), Level: 0}

	live := []bucketindex.Entry{ancestor, bystander, frag("a", 3)}

	assert.Empty(t, bucketindex.Subsumed(live, nil), "an incomplete group retires nothing")

	got := bucketindex.Subsumed(live, []bucketindex.Entry{frag("b", 4)})
	assert.Contains(t, got, "old", "the completed group holds the ancestor's rows")
	assert.NotContains(t, got, "by", "and nothing else")
	assert.NotContains(t, got, "a", "least of all its own members")
}

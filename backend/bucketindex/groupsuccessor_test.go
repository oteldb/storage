package bucketindex_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func members3(name string, first uint64, claimed bucketindex.Interval, level uint32) []bucketindex.Entry {
	c := bucketindex.Claim{Blocks: claimed, Group: bucketindex.Range(0, first, first+2)}
	out := make([]bucketindex.Entry, 3)

	for i := range out {
		out[i] = bucketindex.Entry{
			Prefix: name + string(rune('0'+i)), Blocks: bucketindex.Blocks(first + uint64(i)), Claim: c, Level: level,
		}
	}

	return out
}

func prefixSet(entries ...bucketindex.Entry) map[string]struct{} {
	out := make(map[string]struct{}, len(entries))
	for i := range entries {
		out[entries[i].Prefix] = struct{}{}
	}

	return out
}

// TestSuccessorOfAGroupSubsumesEveryMember pins #721: a part holding a split group's whole claimed
// ancestry at a higher level holds every member's rows, though it contains none of their own blocks.
func TestSuccessorOfAGroupSubsumesEveryMember(t *testing.T) {
	t.Parallel()

	group := members3("f", 10, bucketindex.Blocks(1), 1)
	succ := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Range(0, 1, 2), Level: 2}

	for _, m := range group {
		assert.True(t, succ.Supersedes(m), "%s holds %s's rows", succ.Prefix, m.Prefix)
	}

	assert.Equal(t, prefixSet(group...), bucketindex.Subsumed(group, []bucketindex.Entry{succ}))

	lost := bucketindex.WantOf(group[1], bucketindex.Generation{})
	got, ok := (&bucketindex.Index{Entries: []bucketindex.Entry{succ}}).Satisfying(lost)
	require.True(t, ok, "a want for a member is answered by a part holding its group's ancestry")
	assert.Equal(t, "s", got.Prefix)

	hole := group[1]
	hole.Hole = true
	assert.True(t, bucketindex.Revokes(succ, hole))

	t.Run("NotAbove", func(t *testing.T) {
		t.Parallel()

		level := succ
		level.Level = 1
		assert.False(t, level.Supersedes(group[0]), "an ancestor or a rival at the group's level is no successor")
		assert.Empty(t, bucketindex.Subsumed(group, []bucketindex.Entry{level}))
	})

	t.Run("PartOfTheClaim", func(t *testing.T) {
		t.Parallel()

		wide := members3("g", 10, bucketindex.Range(0, 1, 2), 1)
		part := bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Range(0, 2, 3), Level: 2}
		assert.False(t, part.Supersedes(wide[0]), "block 1's rows are in the group and not in the part")
		assert.Empty(t, bucketindex.Subsumed(wide, []bucketindex.Entry{part}))
	})

	t.Run("MemberMergedWithMore", func(t *testing.T) {
		t.Parallel()

		merged := bucketindex.Entry{
			Prefix: "m", Blocks: bucketindex.Blocks(5, 10), Claim: group[0].Claim, Level: 2,
		}
		succ := succ
		succ.Level = 3
		assert.False(t, succ.Supersedes(merged), "block 5's rows are not in the successor")
	})

	t.Run("Nested", func(t *testing.T) {
		t.Parallel()

		// f2 was merged with block 7 and split again, into g over 20..22.
		inner := members3("g", 20, bucketindex.Blocks(7, 12), 2)
		live := slices.Concat(group[:2], inner)
		succ := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Blocks(1, 2, 7), Level: 3}

		assert.Equal(t, prefixSet(live...), bucketindex.Subsumed(live, []bucketindex.Entry{succ}),
			"the outer claim yields f2, which with block 7 yields the inner claim")

		short := succ
		short.Blocks = bucketindex.Blocks(1, 2)
		assert.Equal(t, prefixSet(group[:2]...), bucketindex.Subsumed(live, []bucketindex.Entry{short}),
			"without block 7 the inner group holds rows the successor lacks")
	})
}

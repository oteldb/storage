package bucketindex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestCheckTenure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                      string
		stamped, current, indexed uint64
		ok                        bool
	}{
		{"current tenure", 5, 5, 5, true},
		{"first commit of a new tenure", 5, 5, 3, true},
		{"legacy index", 5, 5, 0, true},
		{"no claim", 5, 0, 3, false},
		{"never held", 0, 0, 0, false},
		{"tenure restarted mid-operation", 5, 7, 5, false},
		{"a later tenure wrote the index", 5, 5, 6, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := bucketindex.CheckTenure(tc.stamped, tc.current, tc.indexed)
			if tc.ok {
				assert.NoError(t, err)

				return
			}

			assert.ErrorIs(t, err, bucketindex.ErrSuperseded)
		})
	}
}

func TestAllocatorAssignsUnderItsTerm(t *testing.T) {
	t.Parallel()

	a := bucketindex.NewAllocator(bucketindex.Block{Term: 9, N: 4})
	assert.EqualValues(t, 9, a.Term())
	assert.Equal(t, bucketindex.Block{Term: 9, N: 3}, a.Mark(), "nothing assigned yet")

	flush, _ := a.Assign(bucketindex.FlushPlan())
	assert.Equal(t, bucketindex.TermBlocks(9, 4), flush)

	src := []bucketindex.Entry{
		{Blocks: bucketindex.TermBlocks(2, 1)},
		{Blocks: flush, Level: 1},
	}

	single := bucketindex.PlanMerge(src, 1)
	require.Len(t, single, 1)
	blocks, claim := a.Assign(single[0])
	assert.Equal(t, bucketindex.TermBlocks(2, 1).Union(flush), blocks, "a whole merge inherits across tenures")
	assert.False(t, claim.Valid())
	assert.EqualValues(t, 2, single[0].Level)

	split := bucketindex.PlanMerge(src, 3)
	var group bucketindex.Claim

	for i, p := range split {
		b, c := a.Assign(p)
		assert.Equal(t, bucketindex.TermBlocks(9, uint64(5+i)), b, "fragment %d", i)

		group = c
	}

	assert.Equal(t, bucketindex.Claim{Blocks: blocks, Group: bucketindex.Range(9, 5, 7)}, group)
	assert.Equal(t, bucketindex.Block{Term: 9, N: 7}, a.Mark())
}

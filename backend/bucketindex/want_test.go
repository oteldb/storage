package bucketindex_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestRecordWantKeepsSortedAndReplaces(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.RecordWant(bucketindex.Want{Prefix: "c", Blocks: bucketindex.Range(0, 3, 3)})
	ix.RecordWant(bucketindex.Want{Prefix: "a", Blocks: bucketindex.Range(0, 1, 1)})
	ix.RecordWant(bucketindex.Want{Prefix: "b", Blocks: bucketindex.Range(0, 2, 2)})

	prefixes := []string{ix.Wanted[0].Prefix, ix.Wanted[1].Prefix, ix.Wanted[2].Prefix}
	assert.Equal(t, []string{"a", "b", "c"}, prefixes)

	ix.RecordWant(bucketindex.Want{Prefix: "b", Generation: bucketindex.Generation{Term: 2, Counter: 7}})
	require.Len(t, ix.Wanted, 3)
	assert.Equal(t, bucketindex.Generation{Term: 2, Counter: 7}, ix.Wanted[1].Generation)
}

func TestSatisfyWant(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.RecordWant(bucketindex.Want{Prefix: "a"})
	ix.RecordWant(bucketindex.Want{Prefix: "b"})

	assert.True(t, ix.SatisfyWant("a"))
	assert.False(t, ix.SatisfyWant("a"), "already discharged")
	require.Len(t, ix.Wanted, 1)
	assert.Equal(t, "b", ix.Wanted[0].Prefix)
}

func TestWants(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	assert.Empty(t, ix.Wants())

	w := bucketindex.Want{Prefix: "p", Blocks: bucketindex.Range(0, 4, 4)}
	ix.RecordWant(w)
	assert.Equal(t, map[string]bucketindex.Want{"p": w}, ix.Wants())
}

func TestTrimWantsDropsDischarged(t *testing.T) {
	t.Parallel()

	wants := []bucketindex.Want{
		{Prefix: "back", Blocks: bucketindex.Range(0, 1, 1)},
		{Prefix: "merged", Blocks: bucketindex.Range(0, 2, 2)},
		{Prefix: "still-gone", Blocks: bucketindex.Range(0, 9, 9)},
	}
	live := []bucketindex.Entry{
		{Prefix: "back", Blocks: bucketindex.Range(0, 1, 1)},
		{Prefix: "successor", Blocks: bucketindex.Range(0, 2, 5), Level: 1},
	}

	kept := bucketindex.TrimWants(wants, live)
	require.Len(t, kept, 1)
	assert.Equal(t, "still-gone", kept[0].Prefix)
}

// TestTrimWantsKeepsEveryOutstandingWant pins that no count trims an outstanding want, MaxWants
// included: a want is the only record that its part is owed, and past the horizon the record is what
// a reseed will need.
func TestTrimWantsKeepsEveryOutstandingWant(t *testing.T) {
	t.Parallel()

	n := bucketindex.MaxWants + 7
	wants := make([]bucketindex.Want, 0, n)

	for i := range n {
		wants = append(wants, bucketindex.Want{
			Prefix:     fmt.Sprintf("p%05d", n-i),
			Generation: bucketindex.Generation{Term: 1, Counter: uint64(i + 1)},
		})
	}

	kept := bucketindex.TrimWants(wants, nil)
	require.Len(t, kept, n, "nothing vanishes")
	assert.True(t, slices.IsSortedFunc(kept, func(a, b bucketindex.Want) int {
		return strings.Compare(a.Prefix, b.Prefix)
	}), "prefix order keeps the encoding deterministic")
}

func TestMaxWantsMatchesMaxRemovals(t *testing.T) {
	t.Parallel()

	assert.Equal(t, bucketindex.MaxRemovals, bucketindex.MaxWants,
		"one constant governs the tombstone horizon and the reseed boundary")
}

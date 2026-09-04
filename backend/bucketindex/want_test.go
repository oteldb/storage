package bucketindex_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestRecordWantKeepsSortedAndReplaces(t *testing.T) {
	t.Parallel()

	var ix bucketindex.Index
	ix.RecordWant(bucketindex.Want{Prefix: "c", Blocks: bucketindex.Interval{Min: 3, Max: 3}})
	ix.RecordWant(bucketindex.Want{Prefix: "a", Blocks: bucketindex.Interval{Min: 1, Max: 1}})
	ix.RecordWant(bucketindex.Want{Prefix: "b", Blocks: bucketindex.Interval{Min: 2, Max: 2}})

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

	w := bucketindex.Want{Prefix: "p", Blocks: bucketindex.Interval{Min: 4, Max: 4}}
	ix.RecordWant(w)
	assert.Equal(t, map[string]bucketindex.Want{"p": w}, ix.Wants())
}

func TestTrimWantsDropsDischarged(t *testing.T) {
	t.Parallel()

	wants := []bucketindex.Want{
		{Prefix: "back", Blocks: bucketindex.Interval{Min: 1, Max: 1}},
		{Prefix: "merged", Blocks: bucketindex.Interval{Min: 2, Max: 2}},
		{Prefix: "still-gone", Blocks: bucketindex.Interval{Min: 9, Max: 9}},
	}
	live := []bucketindex.Entry{
		{Prefix: "back", Blocks: bucketindex.Interval{Min: 1, Max: 1}},
		{Prefix: "successor", Blocks: bucketindex.Interval{Min: 2, Max: 5}, Level: 1},
	}

	kept, dropped := bucketindex.TrimWants(wants, live, bucketindex.MaxWants)
	require.Len(t, kept, 1)
	assert.Equal(t, "still-gone", kept[0].Prefix)
	assert.Empty(t, dropped, "discharged wants are not losses")
}

// TestTrimWantsBoundReports pins the reason the bound is safe: what it forces out is handed back,
// so exceeding it escalates rather than silently forgetting a repair.
func TestTrimWantsBoundReports(t *testing.T) {
	t.Parallel()

	wants := make([]bucketindex.Want, 0, 10)
	for i := range 10 {
		wants = append(wants, bucketindex.Want{
			Prefix:     fmt.Sprintf("p%02d", i),
			Generation: bucketindex.Generation{Term: 1, Counter: uint64(i + 1)},
		})
	}

	kept, dropped := bucketindex.TrimWants(wants, nil, 4)
	require.Len(t, kept, 4)
	require.Len(t, dropped, 6)

	assert.Equal(t, []string{"p00", "p01", "p02", "p03"},
		[]string{kept[0].Prefix, kept[1].Prefix, kept[2].Prefix, kept[3].Prefix},
		"the oldest obligations survive")
	assert.Equal(t, "p04", dropped[0].Prefix)
	assert.Equal(t, len(wants), len(kept)+len(dropped), "nothing vanishes")
}

func TestMaxWantsMatchesMaxRemovals(t *testing.T) {
	t.Parallel()

	assert.Equal(t, bucketindex.MaxRemovals, bucketindex.MaxWants,
		"one constant governs the tombstone horizon and the reseed boundary")
}

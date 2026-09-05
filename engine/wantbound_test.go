package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// lostWindow is the time range the i-th seeded part covers; the ranges are disjoint so a dropped
// want leaves a window no other want disclaims.
func lostWindow(i int) (int64, int64) {
	start := int64(i+1) * 1_000

	return start, start + 999
}

// seedGoneParts commits an index naming n parts that have no objects, the state a node is in after
// losing n parts from its disk, and returns their prefixes.
func seedGoneParts(t *testing.T, be backend.Backend, n int) []string {
	t.Helper()

	ix := bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	lost := make([]string, 0, n)

	for i := range n {
		prefix := fmt.Sprintf("%s/%016d", lostPrefix, i+1)
		start, end := lostWindow(i)
		ix.Add(bucketindex.Entry{
			Prefix: prefix, MinTime: start, MaxTime: end,
			Blocks: bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)},
		})
		lost = append(lost, prefix)
	}

	_, err := ix.Save(context.Background(), be, lostIndexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	return lost
}

// owedParts returns every part the committed index still accounts for as lost: outstanding wants and
// acknowledged holes. A part in neither has left Entries into nothing, which is silent loss.
func owedParts(ix *bucketindex.Index) map[string]struct{} {
	owed := make(map[string]struct{}, len(ix.Wanted))
	for _, w := range ix.Wanted {
		owed[w.Prefix] = struct{}{}
	}

	for _, h := range ix.Holes() {
		owed[h.Prefix] = struct{}{}
	}

	return owed
}

// TestWantsPastBoundStayOwed pins the invariant a part leaves Entries only into Removed or into
// Wanted, past the point where the index carries more wants than MaxWants: no lost part may vanish
// from the index without a want, a hole, or a raised LostParts, and every lost window stays disclaimed.
func TestWantsPastBoundStayOwed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	lost := seedGoneParts(t, be, bucketindex.MaxWants+1)

	e := newLostEngine(be)
	require.NoError(t, e.LoadParts(ctx))

	check := func(when string) {
		t.Helper()

		ix := loadIndex(t, be, lostPrefix)
		require.Empty(t, ix.Removed, "%s: a loss is not a removal", when)
		assert.Equal(t, uint64(len(ix.Holes())), ix.LostParts, "%s: every hole is a counted loss", when)

		owed := owedParts(ix)

		var forgotten []string

		for i, prefix := range lost {
			if _, ok := owed[prefix]; !ok {
				forgotten = append(forgotten, prefix)
			}

			start, end := lostWindow(i)
			assert.Truef(t, e.WantOverlaps(start, end), "%s: window of %s is served short", when, prefix)
		}

		assert.Emptyf(t, forgotten, "%s: %d of %d lost parts left the index into neither Wanted nor a hole",
			when, len(forgotten), len(lost))
	}

	check("after load")

	mustAppend(t, e, mkSeries("job", "api"), 1, 1.0)
	require.NoError(t, e.Flush(ctx))
	check("after flush")
}

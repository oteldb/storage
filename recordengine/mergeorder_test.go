package recordengine_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

type orderedRow struct {
	ts   int64
	body string
}

// mergeToOnePart flushes each run of rows as its own part of one stream, oldest first, then merges
// until a single part remains. retainFrom is passed to every merge.
func mergeToOnePart(t *testing.T, runs [][]int64, retainFrom int64) (*recordengine.Engine, []orderedRow) {
	t.Helper()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	var all []orderedRow

	for p, run := range runs {
		var recs []rrec

		for r, ts := range run {
			body := fmt.Sprintf("p%d-r%d", p, r)
			recs = append(recs, rrec{ts: ts, body: body})
			all = append(all, orderedRow{ts: ts, body: body})
		}

		ingest(t, e, mkBatch("api", recs...))
		require.NoError(t, e.Flush(ctx))
	}

	for range runs {
		if e.PartCount() <= 1 {
			break
		}

		require.NoError(t, e.Merge(ctx, retainFrom))
	}

	require.Equal(t, 1, e.PartCount(), "the parts must merge into one")

	return e, all
}

// TestMergeOrdersEqualTimestampsByPartThenRow pins the merge's row order when parts overlap in time: by
// timestamp, and among equal timestamps by part (oldest first), then by row within the part. It is the
// order a stable sort of the oldest-to-newest concatenation gives, which is what any change to how a
// merge orders rows must keep.
func TestMergeOrdersEqualTimestampsByPartThenRow(t *testing.T) {
	t.Parallel()

	e, all := mergeToOnePart(t, [][]int64{
		{10, 20, 20, 30},
		{5, 20, 25},
		{20, 20, 40},
	}, 0)

	slices.SortStableFunc(all, func(a, b orderedRow) int { return cmp.Compare(a.ts, b.ts) })

	want := make([]string, len(all))
	for i, r := range all {
		want[i] = r.body
	}

	assert.Equal(t, want, streamBodies(t, e))
}

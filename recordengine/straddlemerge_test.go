package recordengine_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// TestStraddlerMergeHoldsResidentShare bounds the decoded rows one large stream spread over many days
// makes the open day writers take together before one is sealed: the merge's share, overshot by at
// most one append, not a part's worth per day.
//
//nolint:paralleltest // sets the package-global resident observer
func TestStraddlerMergeHoldsResidentShare(t *testing.T) {
	const step = int64(10 * time.Minute)

	body := strings.Repeat("x", 200)

	for _, days := range []int{17, 32} { //nolint:paralleltest // shares the observer
		t.Run(fmt.Sprintf("%d days", days), func(t *testing.T) {
			var peak, run, limit int64

			defer recordengine.SetMergeResidentObserver(func(p, r, l int64) {
				peak, run, limit = max(peak, p), max(run, r), l
			})()

			ctx := context.Background()
			e := recordengine.New(recordengine.Config{
				Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", MaxPartBytes: 16 << 10,
			})

			n := days * 24 * 6
			recs := make([]rrec, 0, n)

			for i := range n {
				recs = append(recs, rrec{ts: int64(i) * step, body: body})
			}

			ingest(t, e, mkBatch("api", recs...))
			require.NoError(t, e.Flush(ctx))

			for range 3 * days {
				require.NoError(t, e.Merge(ctx, 0))
			}

			require.Positive(t, limit)
			assert.LessOrEqual(t, peak, limit+run, "the day writers outgrew the resident share")
			assert.Less(t, run, limit, "one day's run must fit the share for the bound to mean anything")
			assert.Greater(t, peak, limit/2, "the writers must come near the share for the bound to be tested")

			got := 0
			for _, b := range fetchAll(t, e, req("api")) {
				got += len(bodies(b))
			}

			assert.Equal(t, n, got)
		})
	}
}

// TestRetentionRewriteHoldsResidentShare bounds one stream's single day: retention forces every part
// of its bucket, and each merge takes as many as the cap admits, so the stream's surviving rows of
// that day are one run about the size of the resident share. The writers must still peak at the
// share plus a fixed fraction of it, and the backlog must drain.
//
//nolint:paralleltest // sets the package-global resident observer
func TestRetentionRewriteHoldsResidentShare(t *testing.T) {
	const (
		parts = 40
		step  = int64(time.Minute)
	)

	var peak, limit int64

	defer recordengine.SetMergeResidentObserver(func(p, _, l int64) { peak, limit = max(peak, p), l })()

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", MaxPartBytes: 16 << 10,
	})

	body := strings.Repeat("x", 200)
	stored := 0

	for p := range parts {
		recs := make([]rrec, 0, 51)
		recs = append(recs, rrec{ts: int64(p), body: body})
		for i := range 50 {
			recs = append(recs, rrec{ts: int64(2*60+i)*step + int64(p), body: body})
		}

		ingest(t, e, mkBatch("api", recs...))
		require.NoError(t, e.Flush(ctx))

		stored += len(recs)
	}

	require.Len(t, e.Parts(), parts, "one part per flush, each reaching back past the cutoff")

	cutoff := int64(time.Hour)
	for cycle := 0; slices.ContainsFunc(e.Parts(), func(p recordengine.PartStat) bool { return p.MinTime < cutoff }); cycle++ {
		require.Less(t, cycle, parts, "every merge must rewrite at least one forced part")
		require.NoError(t, e.Merge(ctx, cutoff))
	}

	require.Positive(t, limit)
	assert.LessOrEqual(t, peak, limit+limit/4+1024, "the day's run must be routed in bounded pieces")

	got := 0
	for _, b := range fetchAll(t, e, req("api")) {
		got += len(bodies(b))
	}

	assert.Equal(t, stored-parts, got, "retention drops the one expired record of each part")
}

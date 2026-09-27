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

// TestStraddlerMergeHoldsResidentShare bounds what one large stream spread over many days makes the
// open day writers hold in RAM together: the merge's share, overshot by at most one append, not a
// writer's worth per day. Every part stays within the cap too.
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
				MergeMemoryBytes: straddleShare,
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
			assertPartsWithinCap(t, e, straddleShare/3)

			got := 0
			for _, b := range fetchAll(t, e, req("api")) {
				got += len(bodies(b))
			}

			assert.Equal(t, n, got)
		})
	}
}

// straddleShare is the merge memory the straddler tests give: small enough that the open day writers
// reach it, so the bound is exercised.
const straddleShare = 192 << 10

// assertPartsWithinCap asserts every part is within the merge cap plus the one append the cap is
// checked after — at most a quarter of it.
func assertPartsWithinCap(t *testing.T, e *recordengine.Engine, capBytes int64) {
	t.Helper()

	for _, p := range e.Parts() {
		assert.LessOrEqual(t, p.SizeBytes, capBytes+capBytes/4+1024, "part %s outgrew the cap", p.ID)
	}
}

// TestRetentionRewriteHoldsResidentShare bounds one stream's single day: retention forces every part
// of its bucket, and each merge takes as many as the cap admits, so the stream's surviving rows of
// that day are one run about the size of the cap. It is written in parts within the cap, the writers
// within the resident share, and the backlog must drain.
//
//nolint:paralleltest // sets the package-global resident observer
func TestRetentionRewriteHoldsResidentShare(t *testing.T) {
	const (
		parts = 40
		step  = int64(time.Minute)
	)

	var peak, run, limit int64

	defer recordengine.SetMergeResidentObserver(func(p, r, l int64) { peak, run, limit = max(peak, p), max(run, r), l })()

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs", MaxPartBytes: 16 << 10,
		MergeMemoryBytes: straddleShare,
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
	assert.LessOrEqual(t, peak, limit+run, "the writers outgrew the resident share")
	assert.Greater(t, e.PartCount(), 2, "the day's run must be written in several parts")
	assertPartsWithinCap(t, e, straddleShare/3)

	got := 0
	for _, b := range fetchAll(t, e, req("api")) {
		got += len(bodies(b))
	}

	assert.Equal(t, stored-parts, got, "retention drops the one expired record of each part")
}

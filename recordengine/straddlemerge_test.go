package recordengine_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

const (
	straddleHour = int64(time.Hour)
	straddleDay  = 24 * straddleHour
	// straddleStale is the timestamp a producer keeps re-exporting, pinning every part's MinTime.
	straddleStale = 9 * straddleHour
)

// flushStraddler flushes one part in the shape the exemplar stand produced: a re-exported stale
// record plus a fresh one, so the part spans from straddleStale to now.
func flushStraddler(t *testing.T, e *recordengine.Engine, now int64, i int) {
	t.Helper()

	ingest(t, e, mkBatch("api",
		rrec{ts: straddleStale, body: "stale"},
		rrec{ts: now, body: fmt.Sprintf("fresh-%d", i)},
	))
	require.NoError(t, e.Flush(context.Background()))
}

// requireAligned fails when a part crosses a top-level (day) boundary.
func requireAligned(t *testing.T, e *recordengine.Engine) {
	t.Helper()

	for _, p := range e.Parts() {
		require.Equal(t, p.MinTime/straddleDay, p.MaxTime/straddleDay,
			"part %s spans [%v, %v], across a day boundary",
			p.ID, time.Duration(p.MinTime), time.Duration(p.MaxTime))
	}
}

// storedRecords returns every stored record as "ts/body", sorted, so a merge can be checked to
// neither drop nor invent one.
func storedRecords(t *testing.T, e *recordengine.Engine) []string {
	t.Helper()

	var out []string

	for _, b := range fetchAll(t, e, req("api")) {
		for i, body := range bodies(b) {
			out = append(out, fmt.Sprintf("%d/%s", b.Timestamps[i], body))
		}
	}

	slices.Sort(out)

	return out
}

// TestMergeSplitsStraddlerBacklog is the stand's backlog after the fix: every part straddles, and
// merging must converge to day-aligned parts, fewer of them, holding the same records.
func TestMergeSplitsStraddlerBacklog(t *testing.T) {
	t.Parallel()

	const flushes = 64

	e := newEngine(t, backend.Memory())

	for i := range flushes {
		flushStraddler(t, e, 2*straddleDay+int64(i)*int64(30*time.Minute), i)
	}

	require.Len(t, e.Parts(), flushes)
	require.Positive(t, e.MergeShape().Candidates, "straddlers are merge candidates")

	want := storedRecords(t, e)
	cycles := mergeToConvergence(t, e)

	requireAligned(t, e)
	assert.Equal(t, want, storedRecords(t, e), "splitting must neither drop nor duplicate a record")
	assert.LessOrEqual(t, len(e.Parts()), 4, "the backlog must collapse to a few parts per day")

	t.Logf("%d straddlers converged in %d cycles to %d parts", flushes, cycles, len(e.Parts()))
}

// TestMergeKeepsStraddlersFromAccumulating runs the stand's steady state: a merge after every flush
// while one producer keeps re-exporting its stale record. The part count must stay bounded rather
// than grow a part per flush.
func TestMergeKeepsStraddlersFromAccumulating(t *testing.T) {
	t.Parallel()

	const flushes = 192

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	peak := 0

	for i := range flushes {
		flushStraddler(t, e, 2*straddleDay+int64(i)*int64(30*time.Minute), i)
		require.NoError(t, e.Merge(ctx, 0))

		peak = max(peak, len(e.Parts()))
	}

	mergeToConvergence(t, e)
	requireAligned(t, e)

	days := int(int64(flushes) * int64(30*time.Minute) / straddleDay)
	assert.LessOrEqual(t, peak, 4*(days+1), "%d flushes over %d days peaked at %d parts", flushes, days, peak)

	t.Logf("%d flushes over %d days: peak %d parts, settled at %d", flushes, days, peak, len(e.Parts()))
}

// TestForceReachesStraddlers is the #450 escape: parts that fit no ladder level were invisible to
// every group, forced or not, so [recordengine.MergeOptions.Force] was a no-op on them.
func TestForceReachesStraddlers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	for i := range 5 {
		flushStraddler(t, e, straddleDay+int64(i)*int64(time.Minute), i)
	}

	require.Equal(t, 5, e.MergeShape().ForceCandidates)

	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	requireAligned(t, e)
	assert.Len(t, e.Parts(), 2, "five straddlers of two days split into one part per day")
}

// TestRetentionSplitsStraddler checks a retention rewrite of a straddler both drops the expired
// records and splits what survives on day boundaries.
func TestRetentionSplitsStraddler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api",
		rrec{ts: straddleStale, body: "stale"},
		rrec{ts: straddleDay + straddleHour, body: "a"},
		rrec{ts: 2*straddleDay + straddleHour, body: "b"},
	))
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, straddleDay))

	requireAligned(t, e)
	assert.Len(t, e.Parts(), 2)
	want := []string{
		fmt.Sprintf("%d/a", straddleDay+straddleHour),
		fmt.Sprintf("%d/b", 2*straddleDay+straddleHour),
	}
	slices.Sort(want)
	assert.Equal(t, want, storedRecords(t, e))
}

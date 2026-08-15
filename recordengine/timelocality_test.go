package recordengine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// mergeToConvergence runs merge cycles until the part count stops changing, so the assertions
// describe the steady state a long-running engine settles into rather than one arbitrary cycle. The
// bound is a backstop: a selector that oscillates would otherwise hang the test rather than fail it.
func mergeToConvergence(t *testing.T, e *recordengine.Engine) int {
	t.Helper()

	ctx := context.Background()
	prev := -1

	for cycle := range 500 {
		require.NoError(t, e.Merge(ctx, 0))

		n := len(e.Parts())
		if n == prev {
			return cycle
		}

		prev = n
	}

	t.Fatal("merge did not converge in 500 cycles")

	return 0
}

// TestMergeKeepsPartsTimeLocal is the record-engine half of issue #308, stated end to end: ingest
// two days an hour at a time, compact to convergence, and no surviving part may span more than the
// top ladder level. Before time bucketing this collapsed to a single part covering both days, so
// every log query — which is overwhelmingly a narrow recent window — opened all of it.
func TestMergeKeepsPartsTimeLocal(t *testing.T) {
	t.Parallel()

	const (
		hour  = int64(time.Hour)
		hours = 48
	)

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	for h := range hours {
		base := int64(h) * hour
		ingest(t, e, mkBatch("api",
			rrec{ts: base, body: "a"},
			rrec{ts: base + int64(30*time.Minute), body: "b"},
		))
		require.NoError(t, e.Flush(ctx))
	}

	require.Len(t, e.Parts(), hours, "one part per flush before compaction")

	cycles := mergeToConvergence(t, e)

	stats := e.Parts()
	require.NotEmpty(t, stats)

	const top = 24 * hour

	widest := int64(0)
	for _, p := range stats {
		widest = max(widest, p.MaxTime-p.MinTime)
	}

	assert.LessOrEqual(t, widest, top,
		"widest part spans %s, past the %s top ladder level",
		time.Duration(widest), time.Duration(top))

	t.Logf("converged in %d cycles: %d parts, widest span %s",
		cycles, len(stats), time.Duration(widest))
}

// TestMergeStillBoundsPartCount guards the other side of the trade. Bucketing restricts what may
// merge, so it could bound part span by letting part count grow without limit instead — a worse
// problem than the one being fixed. Records are burstier than samples (a service can idle for hours
// then flood), so this uses an uneven flush distribution rather than a regular one.
func TestMergeStillBoundsPartCount(t *testing.T) {
	t.Parallel()

	const hour = int64(time.Hour)

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	flushes := 0

	for h := range 48 {
		// A burst every sixth hour, a trickle otherwise: the shape a real log stream has.
		perHour := 1
		if h%6 == 0 {
			perHour = 8
		}

		for i := range perHour {
			ts := int64(h)*hour + int64(i)*int64(time.Minute)
			ingest(t, e, mkBatch("api", rrec{ts: ts, body: "x"}))
			require.NoError(t, e.Flush(ctx))

			flushes++
		}
	}

	require.Len(t, e.Parts(), flushes)

	mergeToConvergence(t, e)

	assert.Less(t, len(e.Parts()), flushes/4,
		"bucketing must not trade part span for unbounded part count")

	t.Logf("%d flushes converged to %d parts", flushes, len(e.Parts()))
}

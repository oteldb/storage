package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
)

// mergeToConvergence runs merge cycles until nothing changes, so the assertions describe the steady
// state a long-running engine settles into rather than one arbitrary cycle. The bound is a
// backstop: a selector that oscillates would otherwise hang the test rather than fail it.
func mergeToConvergence(t *testing.T, e *engine.Engine) int {
	t.Helper()

	ctx := context.Background()
	prev := -1

	for cycle := range 500 {
		require.NoError(t, e.Merge(ctx, 0))

		n := e.PartCount()
		if n == prev {
			return cycle
		}

		prev = n
	}

	t.Fatal("merge did not converge in 500 cycles")

	return 0
}

// partsOpenedPerWindow returns the average and worst-case number of parts a query of the given
// width must open, swept across the store. This is the number issue #308 is about: with no time
// locality every part overlaps every window and the answer is len(parts) whatever the width.
func partsOpenedPerWindow(stats []engine.PartStat, width int64) (avg float64, worst int) {
	lo, hi := int64(1)<<62, -(int64(1) << 62)
	for _, p := range stats {
		lo, hi = min(lo, p.MinTime), max(hi, p.MaxTime)
	}

	total, n := 0, 0

	for start := lo; start+width <= hi; start += width {
		end := start + width
		opened := 0

		for _, p := range stats {
			if p.MaxTime >= start && p.MinTime <= end {
				opened++
			}
		}

		worst = max(worst, opened)
		total += opened
		n++
	}

	if n == 0 {
		return 0, worst
	}

	return float64(total) / float64(n), worst
}

// TestMergeKeepsPartsTimeLocal is the acceptance test for issue #308, stated end to end rather than
// over the selector in isolation: ingest two days an hour at a time, compact to convergence, and no
// surviving part may span more than the top ladder level. Before time bucketing this collapsed to a
// single part covering the whole two days, so a one-hour query opened all of it.
func TestMergeKeepsPartsTimeLocal(t *testing.T) {
	t.Parallel()

	const (
		hour  = int64(time.Hour)
		hours = 48
	)

	ctx := context.Background()
	e := flushEngine()
	s := mkSeries("job", "api")

	for h := range hours {
		for q := range 4 {
			mustAppend(t, e, s, int64(h)*hour+int64(q)*int64(15*time.Minute), float64(q))
		}

		require.NoError(t, e.Flush(ctx))
	}

	require.Equal(t, hours, e.PartCount(), "one part per flush before compaction")

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

	avg, worst := partsOpenedPerWindow(stats, hour)
	t.Logf("converged in %d cycles: %d parts, widest span %s; a 1h query opens %.2f parts on average, %d worst case",
		cycles, len(stats), time.Duration(widest), avg, worst)

	assert.Less(t, avg, float64(len(stats)),
		"a 1h window must not open every part — that is the absence of time locality")
}

// TestMergeStillBoundsPartCount guards the other side of the trade. Bucketing restricts what may
// merge, so it could bound part span by letting part count grow without limit instead — which is
// issue #25, and a worse problem than the one being fixed. Two days of quarter-hourly flushes must
// still collapse to a small multiple of the buckets they occupy, not stay at one part per flush.
func TestMergeStillBoundsPartCount(t *testing.T) {
	t.Parallel()

	const hour = int64(time.Hour)

	ctx := context.Background()
	e := flushEngine()
	s := mkSeries("job", "api")

	flushes := 0

	for h := range 48 {
		for q := range 4 {
			mustAppend(t, e, s, int64(h)*hour+int64(q)*int64(15*time.Minute), float64(q))
			require.NoError(t, e.Flush(ctx))

			flushes++
		}
	}

	require.Equal(t, flushes, e.PartCount())

	mergeToConvergence(t, e)

	// 48h spans two full 24h buckets plus the still-filling one; a handful of parts per bucket is
	// the expected steady state, one part per flush is the failure.
	assert.Less(t, e.PartCount(), flushes/4,
		"bucketing must not trade part span for unbounded part count (issue #25)")

	t.Logf("%d flushes converged to %d parts", flushes, e.PartCount())
}

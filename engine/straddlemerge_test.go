package engine_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

const (
	straddleHour = int64(time.Hour)
	straddleDay  = 24 * straddleHour
	// straddleStale is where a producer's late samples land, pinning every part's MinTime.
	straddleStale = 9 * straddleHour
)

// flushStraddler flushes one part spanning from a stale sample to a fresh one at now.
func flushStraddler(t *testing.T, e *engine.Engine, now int64, i int) {
	t.Helper()

	mustAppend(t, e, mkSeries("job", "stale"), straddleStale+int64(i)*int64(time.Second), float64(i))
	mustAppend(t, e, mkSeries("job", "fresh"), now, float64(i))
	require.NoError(t, e.Flush(context.Background()))
}

// requireAligned fails when a part crosses a top-level (day) boundary.
func requireAligned(t *testing.T, e *engine.Engine) {
	t.Helper()

	for _, p := range e.Parts() {
		require.Equal(t, p.MinTime/straddleDay, p.MaxTime/straddleDay,
			"part %s spans [%v, %v], across a day boundary",
			p.ID, time.Duration(p.MinTime), time.Duration(p.MaxTime))
	}
}

// storedSamples returns every stored sample as "job/ts/value", sorted, so a merge can be checked to
// neither drop nor invent one.
func storedSamples(t *testing.T, e *engine.Engine) []string {
	t.Helper()

	var out []string

	for _, job := range []string{"stale", "fresh"} {
		b := fetchOne(t, e, job)
		for i, ts := range b.Timestamps {
			out = append(out, fmt.Sprintf("%s/%d/%v", job, ts, b.Values[i]))
		}
	}

	slices.Sort(out)

	return out
}

// TestMergeSplitsStraddlerBacklog is a backlog of straddlers after the fix: merging must converge to
// day-aligned parts, fewer of them, holding the same samples.
func TestMergeSplitsStraddlerBacklog(t *testing.T) {
	t.Parallel()

	const flushes = 64

	e := flushEngine()

	for i := range flushes {
		flushStraddler(t, e, 2*straddleDay+int64(i)*int64(30*time.Minute), i)
	}

	require.Equal(t, flushes, e.PartCount())
	require.Positive(t, e.MergeShape().Candidates, "straddlers are merge candidates")

	want := storedSamples(t, e)
	cycles := mergeToConvergence(t, e)

	requireAligned(t, e)
	assert.Equal(t, want, storedSamples(t, e), "splitting must neither drop nor duplicate a sample")
	assert.LessOrEqual(t, e.PartCount(), 4, "the backlog must collapse to a few parts per day")

	t.Logf("%d straddlers converged in %d cycles to %d parts", flushes, cycles, e.PartCount())
}

// TestMergeKeepsStraddlersFromAccumulating runs a merge after every flush while late samples keep
// arriving. The part count must stay bounded rather than grow a part per flush.
func TestMergeKeepsStraddlersFromAccumulating(t *testing.T) {
	t.Parallel()

	const flushes = 192

	ctx := context.Background()
	e := flushEngine()
	peak := 0

	for i := range flushes {
		flushStraddler(t, e, 2*straddleDay+int64(i)*int64(30*time.Minute), i)
		require.NoError(t, e.Merge(ctx, 0))

		peak = max(peak, e.PartCount())
	}

	mergeToConvergence(t, e)
	requireAligned(t, e)

	days := int(int64(flushes) * int64(30*time.Minute) / straddleDay)
	assert.LessOrEqual(t, peak, 4*(days+1), "%d flushes over %d days peaked at %d parts", flushes, days, peak)

	t.Logf("%d flushes over %d days: peak %d parts, settled at %d", flushes, days, peak, e.PartCount())
}

// TestForceReachesStraddlers is the #450 escape: parts that fit no ladder level were invisible to
// every run, forced or not.
func TestForceReachesStraddlers(t *testing.T) {
	t.Parallel()

	e := flushEngine()

	for i := range 5 {
		flushStraddler(t, e, straddleDay+int64(i)*int64(time.Minute), i)
	}

	require.Equal(t, 5, e.MergeShape().ForceCandidates)
	require.NoError(t, e.MergeWith(context.Background(), engine.MergeOptions{Force: true}))

	requireAligned(t, e)
	assert.Equal(t, 2, e.PartCount(), "five straddlers of two days split into one part per day")
}

// TestRetentionSplitsStraddler checks a retention rewrite of a straddler both drops the expired
// samples and splits what survives on day boundaries.
func TestRetentionSplitsStraddler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := flushEngine()

	mustAppend(t, e, mkSeries("job", "stale"), straddleStale, 1)
	mustAppend(t, e, mkSeries("job", "fresh"), straddleDay+straddleHour, 2)
	mustAppend(t, e, mkSeries("job", "fresh"), 2*straddleDay+straddleHour, 3)
	require.NoError(t, e.Flush(ctx))

	require.NoError(t, e.Merge(ctx, straddleDay))

	requireAligned(t, e)
	assert.Equal(t, 2, e.PartCount())
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", "stale")}}),
		"retention must drop the expired sample")
	assert.Equal(t, []int64{straddleDay + straddleHour, 2*straddleDay + straddleHour}, fetchOne(t, e, "fresh").Timestamps)
}

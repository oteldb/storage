package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

const (
	reforceCycles = 10
	dayNanos      = 24 * int64(time.Hour)
)

// reforceOpts rolls every sample older than three days up to one per minute.
func reforceOpts() engine.MergeOptions {
	return engine.MergeOptions{Downsample: []engine.DownsampleTier{
		{Before: 3 * dayNanos, Interval: int64(time.Minute), Agg: signal.AggLast},
	}}
}

// flushSeconds flushes one part holding a sample per second for n seconds from start.
func flushSeconds(t *testing.T, e *engine.Engine, s signal.Series, start, n int64) {
	t.Helper()

	for i := range n {
		mustAppend(t, e, s, start+i*int64(time.Second), float64(i))
	}

	require.NoError(t, e.Flush(context.Background()))
}

func sampleTimes(t *testing.T, e *engine.Engine) []int64 {
	t.Helper()

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)

	return got[0].Timestamps
}

// TestDownsampleRollsUpEveryOldBucket checks every bucket older than the tier's cutoff is rolled up,
// not only the oldest: two parts two days apart, both past the cutoff, each two minutes of
// per-second samples.
func TestDownsampleRollsUpEveryOldBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	flushSeconds(t, e, s, 0, 120)
	flushSeconds(t, e, s, 2*dayNanos, 120)

	for range reforceCycles {
		require.NoError(t, e.MergeWith(ctx, reforceOpts()))
	}

	assert.Equal(t, []int64{0, int64(time.Minute), 2 * dayNanos, 2*dayNanos + int64(time.Minute)}, sampleTimes(t, e),
		"both parts roll up to one sample per minute")
}

// TestDownsampleLeavesLadderRunning checks enabling downsampling does not stop the ladder: once the
// old part is rolled up, two fresh parts sharing an hour must still merge.
func TestDownsampleLeavesLadderRunning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	flushSeconds(t, e, s, 0, 120)
	flushSeconds(t, e, s, 5*dayNanos, 60)
	flushSeconds(t, e, s, 5*dayNanos+int64(time.Minute), 60)
	require.Equal(t, 3, e.PartCount())

	for range reforceCycles {
		require.NoError(t, e.MergeWith(ctx, reforceOpts()))
	}

	assert.Equal(t, 2, e.PartCount(), "the rolled-up part plus the two fresh parts merged into one")
}

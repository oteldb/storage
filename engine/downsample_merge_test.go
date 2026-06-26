package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// listKeys returns the sorted backend keys under the metrics prefix (the part objects).
func listKeys(t *testing.T, b backend.Backend) []string {
	t.Helper()

	keys, err := b.List(context.Background(), "default/metrics")
	require.NoError(t, err)

	return keys
}

// TestMergeWithDownsample rolls up old samples at merge time while leaving recent samples raw, and
// confirms a second merge with the same options is a stable fixed point (no churn, same data).
func TestMergeWithDownsample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := flushEngine()
	s := mkSeries("job", "api")

	// Four old samples spanning two 10-wide buckets ([10] and [20]), plus two recent samples past
	// the Before=100 cutoff. Spread across parts so the merge also compacts.
	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 12, 2)
	mustAppend(t, e, s, 23, 3)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 28, 4)
	mustAppend(t, e, s, 150, 5)
	mustAppend(t, e, s, 160, 6)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 2, e.PartCount())

	opts := engine.MergeOptions{Downsample: []engine.DownsampleTier{
		{Before: 100, Interval: 10, Agg: signal.AggLast},
	}}
	require.NoError(t, e.MergeWith(ctx, opts))
	assert.Equal(t, 1, e.PartCount(), "parts compacted into one")

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	// bucket[10]=last(10,12)=2, bucket[20]=last(23,28)=4, then raw 150,160.
	assert.Equal(t, []int64{10, 20, 150, 160}, got[0].Timestamps)
	assert.Equal(t, []float64{2, 4, 5, 6}, got[0].Values)

	// Re-merging with the same options is a fixed point: still one part, identical data.
	require.NoError(t, e.MergeWith(ctx, opts))
	assert.Equal(t, 1, e.PartCount())
	got2 := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got2, 1)
	assert.Equal(t, got[0].Timestamps, got2[0].Timestamps)
	assert.Equal(t, got[0].Values, got2[0].Values)
}

// TestMergeWithDownsampleSinglePart confirms a lone part is rolled up (downsampleApplies), and that
// once at target resolution a further merge does not rewrite it (the fixed-point churn guard).
func TestMergeWithDownsampleSinglePart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	b := backend.Memory()
	e := engine.New(engine.Config{Backend: b, Prefix: "default/metrics"})
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 14, 2)
	mustAppend(t, e, s, 90, 9)
	require.NoError(t, e.Flush(ctx))

	keysBefore := listKeys(t, b)

	opts := engine.MergeOptions{Downsample: []engine.DownsampleTier{
		{Before: 100, Interval: 10, Agg: signal.AggSum},
	}}
	require.NoError(t, e.MergeWith(ctx, opts))

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 90}, got[0].Timestamps)
	assert.Equal(t, []float64{3, 9}, got[0].Values) // the [10] bucket sums 1 and 2

	keysAfter := listKeys(t, b)
	require.NotEqual(t, keysBefore, keysAfter, "the rolled-up part replaced the original")

	// Second merge: already at target resolution ⇒ no rewrite (same backend objects).
	require.NoError(t, e.MergeWith(ctx, opts))
	assert.Equal(t, keysAfter, listKeys(t, b), "fixed point: no part churn on re-merge")
}

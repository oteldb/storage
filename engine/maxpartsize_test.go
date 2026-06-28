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

func TestMaxPartSizeSplitsFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// ~5 rows per part (partRowBytes ≈ 32).
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", MaxPartBytes: 160})

	const n = 20
	ids, series, ts, vals := distinctSeries(n)

	res, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, n, res.Accepted)

	require.NoError(t, e.Flush(ctx))
	assert.Greater(t, e.PartCount(), 1, "flush split the head into multiple parts under MaxPartBytes")

	// All series are still readable across the split parts.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	assert.Len(t, got, n, "every series readable across the split parts")
}

func TestMaxPartSizeUnlimitedSinglePart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// MaxPartBytes 0 ⇒ unlimited: one part regardless of size (byte-identical to the prior behavior).
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	ids, series, ts, vals := distinctSeries(50)
	_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)

	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, 1, e.PartCount(), "unlimited ⇒ a single part")
}

func TestMaxPartSizeSplitsMerge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// maxRows 5; the merge output splits at the merge cap (mergeHeight × maxRows = 40 rows), so a
	// merge that produces more than the cap is kept split into bounded parts.
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", MaxPartBytes: 160})

	// Enough flushes of the same 10 series that the merge output (50 rows) exceeds the 40-row cap.
	for range 5 {
		ids, series, ts, vals := distinctSeries(10)
		_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))
	assert.Greater(t, e.PartCount(), 1, "merge keeps its output split under the merge cap")

	// Every output part stays within the merge cap.
	for _, p := range e.Parts() {
		assert.LessOrEqual(t, p.Rows, int64(40), "merged output part respects the merge cap")
	}

	// The five flushes wrote the same 10 series (same ids), so the merged set is 10 series.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	assert.Len(t, got, 10, "all series readable after a split merge")
}

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

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", MaxPartBytes: 160})

	// Two flushes, then a merge — the merged output must also stay split under the cap.
	for range 2 {
		ids, series, ts, vals := distinctSeries(10)
		_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	require.NoError(t, e.Merge(ctx, 0))
	assert.Greater(t, e.PartCount(), 1, "merge keeps its output split under MaxPartBytes")

	// The two flushes wrote the same 10 series (same ids), so the merged set is 10 series.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 62})
	assert.Len(t, got, 10, "all series readable after a split merge")
}

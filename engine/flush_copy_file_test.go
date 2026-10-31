package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestFlushCopiedColumnsFileRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := file.New(t.TempDir())
	require.NoError(t, err)
	cfg := engine.Config{Backend: be, Prefix: "default/metrics"}
	e := engine.New(cfg)
	t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
	for _, tc := range []struct {
		name            string
		ts              []int64
		values, weights []float64
	}{
		{"ordered", []int64{1, 2, 3}, []float64{10, 20, 30}, nil},
		{"weighted unordered", []int64{3, 1, 2}, []float64{30, 10, 20}, []float64{3, 1, 2}},
	} {
		series := mkSeries("job", tc.name)
		ids := []signal.SeriesID{series.Hash(), series.Hash(), series.Hash()}
		result, err := e.AppendBatch(ids, tc.ts, tc.values, tc.weights, func(int) signal.Series { return series }, engine.AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, 3, result.Accepted)
	}
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Close(ctx))
	reopened := engine.New(cfg)
	t.Cleanup(func() { require.NoError(t, reopened.Close(ctx)) })
	require.NoError(t, reopened.LoadParts(ctx))
	for _, name := range []string{"ordered", "weighted unordered"} {
		got := fetchAll(t, reopened, fetch.Request{Start: 0, End: 10, Matchers: []fetch.Matcher{eqMatcher("job", name)}})
		require.Len(t, got, 1)
		assert.Equal(t, []int64{1, 2, 3}, got[0].Timestamps)
		assert.Equal(t, []float64{10, 20, 30}, got[0].Values)
		if name == "ordered" {
			assert.Nil(t, got[0].ScaleFactors)
		} else {
			assert.Equal(t, []float64{1, 2, 3}, got[0].ScaleFactors)
		}
	}
}

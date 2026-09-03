package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// seedAggPart flushes one part holding five samples at ts 100..500 with value == ts (sum 1500).
func seedAggPart(ctx context.Context, t *testing.T, e *engine.Engine) {
	t.Helper()

	s := mkSeries("job", "api")
	for ts := int64(100); ts <= 500; ts += 100 {
		mustAppend(t, e, s, ts, float64(ts))
	}

	require.NoError(t, e.Flush(ctx))
}

// TestAggregateWholePartDecodeWithBlockCache pins the whole-part decode convention on the block-cache
// path: the aggregate folds that decode a part without a per-series range (the step bucketer and the
// sidecar-less range fold) must see the part's rows, not an unfilled buffer.
func TestAggregateWholePartDecodeWithBlockCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	// The step bucketer decodes the whole part whenever the part straddles more than one bucket, so
	// the sidecar cannot answer it — reached with the sidecar written.
	t.Run("StepBucketer", func(t *testing.T) {
		t.Parallel()

		e := engine.New(engine.Config{
			Backend: backend.Memory(), Prefix: "default/metrics",
			DecodeCacheBytes: 1 << 20, AggregateStats: true,
		})
		seedAggPart(ctx, t, e)

		got, err := e.AggregateStep(ctx, req, 100)
		require.NoError(t, err)
		require.Len(t, got, 1)

		for _, buckets := range got {
			require.Len(t, buckets, 5, "one bucket per sample")

			for i, b := range buckets {
				want := int64(100 + i*100)
				assert.Equal(t, want, b.Start)
				assert.Equal(t, int64(1), b.Count)
				assert.InDelta(t, float64(want), b.Sum, 0)
			}
		}
	})

	// aggViaStats falls back to a whole-part decode for a part with no sidecar.
	t.Run("SidecarlessRangeFold", func(t *testing.T) {
		t.Parallel()

		e := engine.New(engine.Config{
			Backend: backend.Memory(), Prefix: "default/metrics",
			DecodeCacheBytes: 1 << 20,
		})
		seedAggPart(ctx, t, e)

		got, err := e.AggregateRange(ctx, req)
		require.NoError(t, err)
		require.Len(t, got, 1)

		for _, agg := range got {
			assert.Equal(t, int64(5), agg.Count)
			assert.InDelta(t, 1500.0, agg.Sum, 0)
			assert.InDelta(t, 100.0, agg.Min, 0)
			assert.InDelta(t, 500.0, agg.Max, 0)
		}

		steps, err := e.AggregateStep(ctx, req, 100)
		require.NoError(t, err)
		require.Len(t, steps, 1)

		for _, buckets := range steps {
			assert.Len(t, buckets, 5, "one bucket per sample")
		}
	})
}

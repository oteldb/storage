package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// TestSplitKeepsDownsampleBucketsWhole pins a rollup bucket crossing the day boundary a straddler
// split cuts on: a 7h grid puts 23:00 and 01:00 in one bucket starting at 21:00, and splitting the
// samples by day would emit two partial aggregates at 21:00, of which the read keeps one.
func TestSplitKeepsDownsampleBucketsWhole(t *testing.T) {
	t.Parallel()

	const hour = int64(time.Hour)

	for _, tt := range []struct {
		agg  signal.Aggregation
		want float64
	}{
		{signal.AggSum, 4},
		{signal.AggCount, 2},
		{signal.AggAvg, 2},
		{signal.AggFirst, 1},
		{signal.AggLast, 3},
	} {
		t.Run(tt.agg.String(), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			e := flushEngine()
			api := mkSeries("job", "api")

			mustAppend(t, e, api, 23*hour, 1)
			mustAppend(t, e, api, 25*hour, 3)
			require.NoError(t, e.Flush(ctx))

			tiers := []engine.DownsampleTier{{Before: 1 << 62, Interval: 7 * hour, Agg: tt.agg}}
			require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Downsample: tiers}))

			got := fetchOne(t, e, "api")
			assert.Equal(t, []int64{21 * hour}, got.Timestamps)
			assert.Equal(t, []float64{tt.want}, got.Values)
		})
	}
}

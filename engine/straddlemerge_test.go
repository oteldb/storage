package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/timebucket"
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

// TestSplitAcrossAgeBands pins a straddler whose rollups move samples into an earlier day than the
// ones left raw: two age bands and raw samples over three days, where the 7h band puts 25:00 and
// 25:30 at 21:00 of the day before while 27:00 and 28:00 roll up in their own day. Every rollup must
// land in a part of its own day, or that part straddles, is merged again, and a Count re-counts its
// one representative as 1.
func TestSplitAcrossAgeBands(t *testing.T) {
	t.Parallel()

	const hour = int64(time.Hour)

	for _, tt := range []struct {
		agg          signal.Aggregation
		old, younger float64
	}{
		{signal.AggSum, 3, 7},
		{signal.AggCount, 2, 2},
		{signal.AggAvg, 1.5, 3.5},
		{signal.AggFirst, 1, 3},
		{signal.AggLast, 2, 4},
		{signal.AggMin, 1, 3},
		{signal.AggMax, 2, 4},
	} {
		t.Run(tt.agg.String(), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			e := flushEngine()
			api := mkSeries("job", "api")

			for i, ts := range []int64{25 * hour, 25*hour + hour/2, 27 * hour, 28 * hour, 30 * hour, 49 * hour} {
				mustAppend(t, e, api, ts, float64(i+1))
			}

			require.NoError(t, e.Flush(ctx))

			tiers := []engine.DownsampleTier{
				{Before: 26 * hour, Interval: 7 * hour, Agg: tt.agg},
				{Before: 29 * hour, Interval: 3 * hour, Agg: tt.agg},
			}

			for range 2 {
				require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Downsample: tiers}))
			}

			got := fetchOne(t, e, "api")
			assert.Equal(t, []int64{21 * hour, 27 * hour, 30 * hour, 49 * hour}, got.Timestamps)
			assert.Equal(t, []float64{tt.old, tt.younger, 5, 6}, got.Values)

			for _, p := range e.Parts() {
				_, ok := timebucket.Finest(p.MinTime, p.MaxTime)
				assert.True(t, ok, "part [%v, %v] fits no ladder level",
					time.Duration(p.MinTime), time.Duration(p.MaxTime))
			}
		})
	}
}

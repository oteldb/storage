package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/signal"
)

// TestRerollDuplicateSplitByMaxParts: seventeen raw parts are due in one bucket, one more than a merge
// takes, and the last repeats the sample of the one before it, as an exporter retry does. The first
// merge rolls sixteen, the next rolls the last alone, and the two representatives then fold as if
// the repeated sample were two.
func TestRerollDuplicateSplitByMaxParts(t *testing.T) {
	reproduce.Unfixed(t, dupRollupIssue, "copies of one sample rolled by separate merges fold as two samples")
	t.Parallel()

	for _, agg := range []signal.Aggregation{signal.AggSum, signal.AggCount, signal.AggAvg} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

			r := newRerollEngine(t)
			r.write(rerollBase+2*dayNanos, 1)
			r.flush()

			for i := int64(1); i <= 16; i++ {
				r.write(rerollBase+i*sec, float64(i))
				r.flush()
			}

			r.write(rerollBase+16*sec, 16)
			r.write(rerollBase+17*sec, 17)
			r.flush()

			for range 4 {
				r.merge(opts)
			}

			r.assertOneRollup(opts.Downsample, 0)
		})
	}
}

// TestRerollRecentTierOverRolledRange: a recent tier wide enough to reach a range a merge rolls up
// mirrors raw samples the representative already accounts for, and a read returns both.
func TestRerollRecentTierOverRolledRange(t *testing.T) {
	reproduce.Unfixed(t, dupRollupIssue, "the recent tier's raw copies read beside, and fold into, the representative holding them")
	t.Parallel()

	for _, agg := range []signal.Aggregation{signal.AggSum, signal.AggCount} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

			r := newRerollEngine(t)
			r.e = engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", RecentWindow: dayNanos})

			r.writeRun(rerollBase, 5, 1)
			r.flush()
			r.merge(opts)
			require.Equal(t, 1, r.e.PartCount())

			r.assertOneRollup(opts.Downsample, 0)
		})
	}
}

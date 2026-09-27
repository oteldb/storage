package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// TestRefreshReplicaAfterRollup: a replica that missed the raw part and refreshes only after it was
// rolled up must still trim every sample the rollup covers, including those newer than the
// representative's own timestamp (First, Min and Max pick an earlier sample, and Sum, Avg and Count
// sit at the bucket start). It then serves exactly what the owner serves. The watermark must also
// survive a later rewrite that no longer sees the rolled-up samples: a re-roll next to an older part,
// and a verbatim copy of the marked part (a precision rewrite).
func TestRefreshReplicaAfterRollup(t *testing.T) {
	t.Parallel()

	opts := func(agg signal.Aggregation) engine.MergeOptions {
		return engine.MergeOptions{Downsample: []engine.DownsampleTier{{Before: 100, Interval: 10, Agg: agg}}}
	}

	for _, agg := range []signal.Aggregation{
		signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggLast, signal.AggSum, signal.AggAvg, signal.AggCount,
	} {
		for _, then := range []string{"", "reroll", "copy"} {
			name := agg.String()
			if then != "" {
				name += "/" + then
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				be := backend.Memory()
				owner := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
				replica := engine.New(engine.Config{Backend: be, Prefix: replicaPrefix})
				s := mkSeries("job", "api")

				write := func(ts int64, v float64) {
					mustAppend(t, owner, s, ts, v)
					mustAppend(t, replica, s, ts, v)
				}

				write(11, 1)
				write(12, 9)
				write(15, 5)
				require.NoError(t, owner.Flush(ctx))
				require.NoError(t, owner.MergeWith(ctx, opts(agg)))

				switch then {
				case "reroll":
					write(5, 7)
					require.NoError(t, owner.Flush(ctx))
					require.NoError(t, owner.MergeWith(ctx, opts(agg)))
				case "copy":
					before := owner.Parts()
					o := opts(agg)
					o.Precision = []engine.PrecisionTier{{Before: 1000, Bits: 12}}
					require.NoError(t, owner.MergeWith(ctx, o))
					require.NotEqual(t, before[0].ID, owner.Parts()[0].ID, "the part was rewritten")
				}

				require.Equal(t, 1, owner.PartCount())
				require.NoError(t, replica.RefreshReplica(ctx))
				assert.Zero(t, replica.HeadSampleCount(), "every head sample is covered by the rolled part")

				want, got := fetchJob(t, owner, "api"), fetchJob(t, replica, "api")
				require.Len(t, want, 1)
				require.Len(t, got, 1)
				assert.Equal(t, want[0].Timestamps, got[0].Timestamps)
				assert.Equal(t, want[0].Values, got[0].Values)
			})
		}
	}
}

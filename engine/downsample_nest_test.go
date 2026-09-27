package engine

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/timebucket"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

var allAggs = []signal.Aggregation{
	signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg, signal.AggCount,
}

// quantizedCutoff mirrors the facade's cutoff: now − after floored to Interval × ⌈1h/Interval⌉.
func quantizedCutoff(now int64, after, interval time.Duration) int64 {
	iv := int64(interval)

	return timebucket.Of(now-int64(after), iv*((int64(time.Hour)+iv-1)/iv))
}

// straddles returns a timestamp whose bucket, in the tier it is rolled into, holds a timestamp
// rolled into another tier — so the bucket's representative would later change tier without its
// samples. ok is false when no timestamp in [lo, hi) does.
func straddles(tiers []DownsampleTier, lo, hi, step int64) (ts int64, ok bool) {
	active := slices.Clone(tiers)
	slices.SortFunc(active, widestFirst)

	for t := lo; t < hi; t += step {
		tier, rolled := pickTier(active, t)
		if !rolled {
			continue
		}

		start := alignDown(t, tier.Interval)
		for _, edge := range []int64{start, start + tier.Interval - 1} {
			if other, _ := pickTier(active, edge); other != tier {
				return t, true
			}
		}
	}

	return 0, false
}

// TestDownsampleNestedCutoffsNeverStraddle checks the contract the facade's cutoffs give the engine:
// with nesting Intervals and each Before a multiple of its own Interval, no bucket of the tier a
// sample is rolled into crosses another tier's range. The non-nesting 7m/1h policy shows why the
// contract is required.
func TestDownsampleNestedCutoffsNeverStraddle(t *testing.T) {
	t.Parallel()

	minute := int64(time.Minute)

	t.Run("NonNesting", func(t *testing.T) {
		t.Parallel()

		tiers := []DownsampleTier{{Before: 126 * minute, Interval: 7 * minute}, {Before: 120 * minute, Interval: 60 * minute}}
		ts, ok := straddles(tiers, 0, 200*minute, minute)
		require.True(t, ok)
		assert.Equal(t, 120*minute, ts)
	})

	t.Run("Nesting", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewPCG(1, 2))

		for range 500 {
			n := 1 + rng.IntN(4)
			tiers := make([]DownsampleTier, n)
			interval := int64(1 + rng.IntN(5))

			for i := range tiers {
				if i > 0 {
					interval *= int64(2 + rng.IntN(4))
				}

				tiers[i] = DownsampleTier{Before: interval * int64(rng.IntN(40)), Interval: interval}
			}

			rng.Shuffle(len(tiers), func(i, j int) { tiers[i], tiers[j] = tiers[j], tiers[i] })

			ts, ok := straddles(tiers, -interval, 2*interval*40, 1)
			require.False(t, ok, "tiers %v straddle at %d", tiers, ts)
		}
	})
}

// TestMergeNestedTiersRollOnce is the 119m/121m/122m case over two merges, on the nesting
// counterpart of 7m + 1h the facade accepts: the result equals one rollup and the second merge is a fixed point.
func TestMergeNestedTiersRollOnce(t *testing.T) {
	t.Parallel()

	minute := int64(time.Minute)
	now := 180*minute + 30*int64(time.Second)
	tiers := []DownsampleTier{
		{Before: quantizedCutoff(now, 0, 5*time.Minute), Interval: 5 * minute},
		{Before: quantizedCutoff(now, time.Hour, time.Hour), Interval: 60 * minute},
	}
	require.Equal(t, []int64{180 * minute, 120 * minute}, []int64{tiers[0].Before, tiers[1].Before})

	ts := []int64{119 * minute, 121 * minute, 122 * minute}
	vals := []float64{100, 1, 3}

	for _, weighted := range []bool{false, true} {
		for _, agg := range allAggs {
			name := agg.String()
			if weighted {
				name += "/weighted"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				var sf []float64
				if weighted {
					sf = []float64{2, 3, 5}
				}

				aggTiers := make([]DownsampleTier, len(tiers))
				for i, tier := range tiers {
					tier.Agg = agg
					aggTiers[i] = tier
				}

				ctx := context.Background()
				e := New(Config{Backend: backend.Memory(), Prefix: "default/metrics"})
				s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
					Key: []byte("job"), Value: signal.StringValue([]byte("api")),
				})}

				w := sf
				if w == nil {
					w = []float64{1, 1, 1}
				}

				for i := range ts {
					_, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, ts[i:i+1], vals[i:i+1], w[i:i+1],
						func(int) signal.Series { return s }, AppendLimits{})
					require.NoError(t, err)
				}

				require.NoError(t, e.Flush(ctx))

				wantTs, wantVals, wantSF := downsample(ts, vals, sf, aggTiers)

				for range 2 {
					require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: aggTiers}))

					it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
					require.NoError(t, err)
					got, err := fetch.Drain(ctx, it)
					require.NoError(t, err)
					require.Len(t, got, 1)

					assert.Equal(t, wantTs, got[0].Timestamps)
					assert.Equal(t, wantVals, got[0].Values)
					assert.Equal(t, wantSF, got[0].ScaleFactors)
				}
			})
		}
	}
}

// TestDownsampleNestedTiersStepwise feeds an ingesting series through a 1m/5m/1h/6h policy one
// quantum at a time and checks the result equals a single rollup at the final cutoffs. Count is left
// out: re-counting a representative is not exact until merges combine representatives (#726).
func TestDownsampleNestedTiersStepwise(t *testing.T) {
	t.Parallel()

	policy := []struct{ after, interval time.Duration }{
		{time.Hour, time.Minute},
		{3 * time.Hour, 5 * time.Minute},
		{6 * time.Hour, time.Hour},
		{12 * time.Hour, 6 * time.Hour},
	}

	hour := int64(time.Hour)
	base := 1000 * 24 * hour
	step := 20 * int64(time.Second)
	end := base + 40*hour

	for _, weighted := range []bool{false, true} {
		for _, agg := range []signal.Aggregation{
			signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg,
		} {
			name := agg.String()
			if weighted {
				name += "/weighted"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				tiersAt := func(now int64) []DownsampleTier {
					out := make([]DownsampleTier, 0, len(policy))
					for _, p := range policy {
						out = append(out, DownsampleTier{Before: quantizedCutoff(now, p.after, p.interval), Interval: int64(p.interval), Agg: agg})
					}

					return out
				}

				var (
					rawTs, ts     []int64
					rawVals, vals []float64
					rawSF, sf     []float64
					next          = base
				)

				for now := base + hour; now <= end; now += hour {
					for ; next < now; next += step {
						v := float64(next / step % 101)
						w := 1.0
						if weighted {
							w = float64(next/step%3 + 1)
						}

						rawTs, rawVals, rawSF = append(rawTs, next), append(rawVals, v), append(rawSF, w)
						ts, vals, sf = append(ts, next), append(vals, v), append(sf, w)
					}

					ts, vals, sf = downsample(ts, vals, sf, tiersAt(now))
					sf = normalizeSF(sf, len(ts))
				}

				wantTs, wantVals, wantSF := downsample(rawTs, rawVals, rawSF, tiersAt(end))
				assert.Equal(t, wantTs, ts)
				if agg == signal.AggAvg {
					assert.InDeltaSlice(t, wantVals, vals, 1e-9, "a weighted mean of means rounds differently")
				} else {
					assert.Equal(t, wantVals, vals)
				}
				assert.Equal(t, normalizeSF(wantSF, len(wantTs)), sf)
			})
		}
	}
}

func normalizeSF(sf []float64, n int) []float64 {
	if sf != nil {
		return sf
	}

	out := make([]float64, n)
	for i := range out {
		out[i] = 1
	}

	return out
}

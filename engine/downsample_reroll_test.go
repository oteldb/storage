package engine_test

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

const rerollIssue = 726

const (
	sec  = int64(time.Second)
	min1 = int64(time.Minute)
	hr   = int64(time.Hour)
	// rerollBase is day-aligned, so the ladder's hour, 6h and day buckets all start on it.
	rerollBase = 10 * dayNanos
)

type rawSample struct {
	ts int64
	v  float64
}

// rerollEngine drives one series through appends, flushes and merges, and records every write so
// the result can be checked against a single rollup of the raw data.
type rerollEngine struct {
	t   *testing.T
	e   *engine.Engine
	s   signal.Series
	raw []rawSample
}

func newRerollEngine(t *testing.T) *rerollEngine {
	t.Helper()

	return &rerollEngine{
		t: t,
		e: engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"}),
		s: mkSeries("job", "api"),
	}
}

func (r *rerollEngine) write(ts int64, v float64) {
	r.t.Helper()
	mustAppend(r.t, r.e, r.s, ts, v)
	r.raw = append(r.raw, rawSample{ts: ts, v: v})
}

// writeRun writes n samples one second apart from start, valued v, v+1, ….
func (r *rerollEngine) writeRun(start int64, n int, v float64) {
	r.t.Helper()

	for i := range n {
		r.write(start+int64(i)*sec, v+float64(i))
	}
}

func (r *rerollEngine) flush() {
	r.t.Helper()
	require.NoError(r.t, r.e.Flush(context.Background()))
}

func (r *rerollEngine) merge(opts engine.MergeOptions) {
	r.t.Helper()
	require.NoError(r.t, r.e.MergeWith(context.Background(), opts))
}

func (r *rerollEngine) got() ([]int64, []float64) {
	r.t.Helper()

	got := fetchAll(r.t, r.e, fetch.Request{Start: -1 << 62, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(r.t, got, 1)

	return got[0].Timestamps, got[0].Values
}

// assertOneRollup checks the stored series equals one rollup of every sample written from
// retainFrom on, under tiers.
func (r *rerollEngine) assertOneRollup(tiers []engine.DownsampleTier, retainFrom int64) {
	r.t.Helper()

	wantTs, wantVals := oneRollup(r.raw, tiers, retainFrom)
	gotTs, gotVals := r.got()
	assert.Equal(r.t, wantTs, gotTs, "timestamps")
	assert.Equal(r.t, wantVals, gotVals, "values")
}

// oneRollup is the oracle: raw samples deduplicated by timestamp (the later write wins), then each
// sample aggregated once into its bucket of the widest tier it is older than. It shares no code with
// the engine's downsample.
func oneRollup(raw []rawSample, tiers []engine.DownsampleTier, retainFrom int64) ([]int64, []float64) {
	latest := make(map[int64]float64, len(raw))
	for _, s := range raw {
		if s.ts >= retainFrom {
			latest[s.ts] = s.v
		}
	}

	type key struct{ interval, start int64 }

	buckets := make(map[key][]rawSample)
	aggs := make(map[key]signal.Aggregation)

	for ts, v := range latest {
		var tier engine.DownsampleTier
		for _, t := range tiers {
			if t.Interval > 0 && ts < t.Before && t.Interval > tier.Interval {
				tier = t
			}
		}

		k := key{start: ts}
		if tier.Interval > 0 {
			k = key{interval: tier.Interval, start: ts - ((ts%tier.Interval)+tier.Interval)%tier.Interval}
			aggs[k] = tier.Agg
		}

		buckets[k] = append(buckets[k], rawSample{ts: ts, v: v})
	}

	keys := make([]key, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b key) int { return cmp.Compare(a.start, b.start) })

	var (
		outTs   []int64
		outVals []float64
	)

	for _, k := range keys {
		ss := buckets[k]
		slices.SortFunc(ss, func(a, b rawSample) int { return cmp.Compare(a.ts, b.ts) })
		outTs = append(outTs, k.start)

		if k.interval == 0 {
			outVals = append(outVals, ss[0].v)

			continue
		}

		outVals = append(outVals, aggregate(aggs[k], ss))
	}

	return outTs, outVals
}

func aggregate(agg signal.Aggregation, ss []rawSample) float64 {
	var sum float64

	lo, hi := ss[0].v, ss[0].v
	for _, s := range ss {
		sum += s.v
		lo, hi = min(lo, s.v), max(hi, s.v)
	}

	switch agg {
	case signal.AggFirst:
		return ss[0].v
	case signal.AggMin:
		return lo
	case signal.AggMax:
		return hi
	case signal.AggSum:
		return sum
	case signal.AggAvg:
		return sum / float64(len(ss))
	case signal.AggCount:
		return float64(len(ss))
	default:
		return ss[len(ss)-1].v
	}
}

func tiersOf(agg signal.Aggregation, tiers ...engine.DownsampleTier) engine.MergeOptions {
	for i := range tiers {
		tiers[i].Agg = agg
	}

	return engine.MergeOptions{Downsample: tiers}
}

// TestRerollCount: a later merge that touches a rolled Count bucket counts its representative as one
// sample. Every path that rewrites rolled data does it: another part in the same ladder bucket, a
// precision rewrite of the part alone, and a coarser tier.
func TestRerollCount(t *testing.T) {
	reproduce.Unfixed(t, rerollIssue, "a Count representative re-rolled by a later merge counts as 1")
	t.Parallel()

	fine := engine.DownsampleTier{Before: rerollBase + hr, Interval: min1}

	t.Run("SameBucket", func(t *testing.T) {
		t.Parallel()

		r := newRerollEngine(t)
		opts := tiersOf(signal.AggCount, fine)

		r.writeRun(rerollBase, 30, 1)
		r.flush()
		r.merge(opts)

		r.writeRun(rerollBase+min1, 30, 1)
		r.flush()
		r.merge(opts)

		r.assertOneRollup(opts.Downsample, 0)
	})

	t.Run("PrecisionRewrite", func(t *testing.T) {
		t.Parallel()

		r := newRerollEngine(t)
		opts := tiersOf(signal.AggCount, fine)

		r.writeRun(rerollBase, 30, 1)
		r.flush()
		r.merge(opts)

		opts.Precision = []engine.PrecisionTier{{Before: rerollBase + dayNanos, Bits: 12}}
		r.merge(opts)

		r.assertOneRollup(opts.Downsample, 0)
	})

	t.Run("Coarsen", func(t *testing.T) {
		t.Parallel()

		r := newRerollEngine(t)
		r.writeRun(rerollBase, 30, 1)
		r.writeRun(rerollBase+min1, 30, 1)
		r.flush()
		r.merge(tiersOf(signal.AggCount, fine))

		coarse := tiersOf(signal.AggCount, fine, engine.DownsampleTier{Before: rerollBase + hr, Interval: hr})
		r.merge(coarse)

		r.assertOneRollup(coarse.Downsample, 0)
	})
}

// TestRerollAvgCoarsen: coarsening Avg representatives weighs every fine bucket equally, whatever its
// population — a mean of means.
func TestRerollAvgCoarsen(t *testing.T) {
	reproduce.Unfixed(t, rerollIssue, "coarsening Avg representatives takes an unweighted mean of means")
	t.Parallel()

	fine := engine.DownsampleTier{Before: rerollBase + hr, Interval: min1}
	coarse := engine.DownsampleTier{Before: rerollBase + hr, Interval: hr}

	r := newRerollEngine(t)
	r.write(rerollBase, 0)
	r.writeRun(rerollBase+min1, 3, 10)
	r.flush()
	r.merge(tiersOf(signal.AggAvg, fine))

	opts := tiersOf(signal.AggAvg, fine, coarse)
	r.merge(opts)

	r.assertOneRollup(opts.Downsample, 0)
}

// TestRerollSameBucketStart: two sources holding a sample at one bucket start keep only the fresher,
// so a representative is dropped rather than combined.
func TestRerollSameBucketStart(t *testing.T) {
	reproduce.Unfixed(t, rerollIssue, "freshest-wins drops one of two samples at a bucket start")
	t.Parallel()

	for _, agg := range []signal.Aggregation{signal.AggSum, signal.AggMin, signal.AggMax, signal.AggCount} {
		// Two parts each roll a different hour of one 6h bucket, so each holds a representative at
		// the bucket start. The second carries a sample from the previous day: that makes it a
		// straddler, which is rewritten alone, and retention later drops the stray day.
		t.Run("TwoRepresentatives/"+agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: 6 * hr})

			r := newRerollEngine(t)
			r.write(rerollBase+min1, 1)
			r.write(rerollBase+2*min1, 9)
			r.flush()
			r.merge(opts)

			r.write(rerollBase-sec, 100)
			r.write(rerollBase+3*hr, 5)
			r.flush()
			r.merge(opts)

			opts.RetainFrom = rerollBase
			r.merge(opts)

			r.assertOneRollup(opts.Downsample, rerollBase)
		})

		// A late raw sample lands exactly on a rolled bucket's start, where no raw sample was.
		t.Run("RawAtStart/"+agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

			r := newRerollEngine(t)
			r.write(rerollBase+sec, 1)
			r.write(rerollBase+2*sec, 9)
			r.flush()
			r.merge(opts)

			r.write(rerollBase, 5)
			r.flush()
			r.merge(opts)

			r.assertOneRollup(opts.Downsample, 0)
		})
	}
}

// TestRerollMixedAgg: tiers with different aggregations compose as the coarse Agg over the fine
// representatives, not over the raw samples: a 1m Sum then 1h Max is the max of per-minute sums.
func TestRerollMixedAgg(t *testing.T) {
	reproduce.Unfixed(t, rerollIssue, "a coarse tier aggregates the fine tier's representatives, not the raw samples")
	t.Parallel()

	fine := engine.DownsampleTier{Before: rerollBase + hr, Interval: min1, Agg: signal.AggSum}
	coarse := engine.DownsampleTier{Before: rerollBase + hr, Interval: hr, Agg: signal.AggMax}

	r := newRerollEngine(t)
	r.write(rerollBase, 4)
	r.write(rerollBase+sec, 5)
	r.write(rerollBase+min1, 8)
	r.flush()
	r.merge(engine.MergeOptions{Downsample: []engine.DownsampleTier{fine}})

	opts := engine.MergeOptions{Downsample: []engine.DownsampleTier{fine, coarse}}
	r.merge(opts)

	r.assertOneRollup(opts.Downsample, 0)
}

// TestRerollLateWrite: a late write into a rolled bucket meets the representative as if it were a
// raw sample. On the representative's timestamp it replaces it; an overwrite of a rolled raw sample
// is aggregated with the representative instead of replacing that sample; and a new sample counts
// the representative as one. Each case lists the aggregations it breaks.
func TestRerollLateWrite(t *testing.T) {
	reproduce.Unfixed(t, rerollIssue, "a late write into a rolled bucket replaces or re-aggregates its representative")
	t.Parallel()

	cases := []struct {
		name string
		ts   int64
		aggs []signal.Aggregation
	}{
		{"OverwriteRepresentative", rerollBase, []signal.Aggregation{signal.AggSum, signal.AggMin, signal.AggAvg, signal.AggCount}},
		{"OverwriteRaw", rerollBase + sec, []signal.Aggregation{signal.AggSum, signal.AggMin, signal.AggAvg}},
		{"NewTimestamp", rerollBase + 2*sec, []signal.Aggregation{signal.AggAvg, signal.AggCount}},
	}

	for _, tc := range cases {
		for _, agg := range tc.aggs {
			t.Run(tc.name+"/"+agg.String(), func(t *testing.T) {
				t.Parallel()

				opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

				r := newRerollEngine(t)
				r.write(rerollBase, 2)
				r.write(rerollBase+sec, 1)
				r.flush()
				r.merge(opts)

				r.write(tc.ts, 5)
				r.flush()
				r.merge(opts)

				r.assertOneRollup(opts.Downsample, 0)
			})
		}
	}
}

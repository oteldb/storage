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

// assertRollsUpTo checks that rolling the stored series up once more gives one rollup of the raw
// data: every representative the stored series holds is exact, even where a bucket still has more
// than one.
func (r *rerollEngine) assertRollsUpTo(tiers []engine.DownsampleTier, retainFrom int64) {
	r.t.Helper()

	gotTs, gotVals := r.got()
	stored := make([]rawSample, len(gotTs))

	for i := range gotTs {
		stored[i] = rawSample{ts: gotTs[i], v: gotVals[i]}
	}

	wantTs, wantVals := oneRollup(r.raw, tiers, retainFrom)
	rolledTs, rolledVals := oneRollup(stored, tiers, retainFrom)
	assert.Equal(r.t, wantTs, rolledTs, "timestamps")
	assert.Equal(r.t, wantVals, rolledVals, "values")
}

// oneRollup is the oracle: raw samples deduplicated by timestamp (the later write wins), then each
// sample aggregated once into its bucket of the widest tier it is older than. Sum, Avg and Count land
// on the bucket start, the others on the chosen sample. It shares no code with the engine's
// downsample.
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

	out := make([]rawSample, 0, len(keys))

	for _, k := range keys {
		ss := buckets[k]
		slices.SortFunc(ss, func(a, b rawSample) int { return cmp.Compare(a.ts, b.ts) })

		if k.interval == 0 {
			out = append(out, ss[0])

			continue
		}

		out = append(out, aggregate(aggs[k], k.start, ss))
	}

	slices.SortFunc(out, func(a, b rawSample) int { return cmp.Compare(a.ts, b.ts) })

	outTs := make([]int64, 0, len(out))
	outVals := make([]float64, 0, len(out))

	for _, s := range out {
		outTs, outVals = append(outTs, s.ts), append(outVals, s.v)
	}

	return outTs, outVals
}

// aggregate folds one bucket's samples, sorted by ts; Min and Max pick the earliest extreme.
func aggregate(agg signal.Aggregation, start int64, ss []rawSample) rawSample {
	var sum float64

	lo, hi := ss[0], ss[0]
	for _, s := range ss {
		sum += s.v

		if s.v < lo.v {
			lo = s
		}

		if s.v > hi.v {
			hi = s
		}
	}

	switch agg {
	case signal.AggFirst:
		return ss[0]
	case signal.AggMin:
		return lo
	case signal.AggMax:
		return hi
	case signal.AggSum:
		return rawSample{ts: start, v: sum}
	case signal.AggAvg:
		return rawSample{ts: start, v: sum / float64(len(ss))}
	case signal.AggCount:
		return rawSample{ts: start, v: float64(len(ss))}
	default:
		return ss[len(ss)-1]
	}
}

func tiersOf(agg signal.Aggregation, tiers ...engine.DownsampleTier) engine.MergeOptions {
	for i := range tiers {
		tiers[i].Agg = agg
	}

	return engine.MergeOptions{Downsample: tiers}
}

// TestRerollCount: a later merge that touches a rolled Count bucket must not count its representative
// as one sample: another part in the same ladder bucket, a precision rewrite of the part alone (a
// verbatim copy under its marker), and a coarser tier.
func TestRerollCount(t *testing.T) {
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

// TestRerollAvgCoarsen: coarsening Avg representatives weighs each fine bucket by its population,
// not equally.
func TestRerollAvgCoarsen(t *testing.T) {
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

// TestRerollSameBucketStart: two sources holding a sample at one bucket start must combine, not keep
// the fresher. Min and Max representatives sit on their chosen sample, so they never meet there.
func TestRerollSameBucketStart(t *testing.T) {
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

			if agg == signal.AggMin || agg == signal.AggMax {
				// The two anchored representatives sit in different ladder buckets and are not merged
				// together yet. Neither is lost, so the merge that does bring them together is exact.
				r.assertRollsUpTo(opts.Downsample, rerollBase)

				return
			}

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

// TestRerollLateSample: a late sample at a new timestamp inside a rolled bucket combines with the
// representative as new data.
func TestRerollLateSample(t *testing.T) {
	t.Parallel()

	for _, agg := range []signal.Aggregation{
		signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg, signal.AggCount,
	} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

			r := newRerollEngine(t)
			r.write(rerollBase, 2)
			r.write(rerollBase+sec, 1)
			r.write(rerollBase+3*sec, 4)
			r.flush()
			r.merge(opts)

			r.write(rerollBase+2*sec, 5)
			r.flush()
			r.merge(opts)

			r.assertOneRollup(opts.Downsample, 0)
		})
	}
}

// TestRerollAggChange: an Agg change applies only to data not yet rolled up. A bucket rolled with Sum
// keeps Sum when a later merge meets it under a Max policy, so a late raw sample folds into the total
// rather than competing with it as a maximum.
func TestRerollAggChange(t *testing.T) {
	t.Parallel()

	sum := tiersOf(signal.AggSum, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})
	maxOpts := tiersOf(signal.AggMax, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

	r := newRerollEngine(t)
	r.write(rerollBase, 2)
	r.write(rerollBase+sec, 3)
	r.flush()
	r.merge(sum)

	r.write(rerollBase+2*sec, 4)
	r.flush()
	r.merge(maxOpts)

	r.assertOneRollup(sum.Downsample, 0)
}

// TestRerollLateOverwrite: a late write reusing the timestamp of a raw sample that is already rolled
// up cannot replace it, because the raw value is gone. Where the overwritten sample was folded into
// the aggregate, the late value is added to it; where it is the representative itself, freshest-wins
// replaces the bucket's whole aggregate.
func TestRerollLateOverwrite(t *testing.T) {
	reproduce.Unfixed(t, 732, "a late overwrite of a rolled sample combines with or replaces the bucket's aggregate")
	t.Parallel()

	cases := []struct {
		name string
		agg  signal.Aggregation
		ts   int64
		v    float64
	}{
		{"FoldedSample/sum", signal.AggSum, rerollBase + sec, 5},
		{"FoldedSample/avg", signal.AggAvg, rerollBase + sec, 5},
		{"Representative/min", signal.AggMin, rerollBase + sec, 5},
		{"Representative/max", signal.AggMax, rerollBase + 3*sec, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(tc.agg, engine.DownsampleTier{Before: rerollBase + hr, Interval: min1})

			r := newRerollEngine(t)
			r.write(rerollBase, 2)
			r.write(rerollBase+sec, 1)
			r.write(rerollBase+3*sec, 4)
			r.flush()
			r.merge(opts)

			r.write(tc.ts, tc.v)
			r.flush()
			r.merge(opts)

			r.assertOneRollup(opts.Downsample, 0)
		})
	}
}

// TestRerollReadFoldsRepresentatives: a query over two unmerged parts that each hold a representative
// of one bucket start, and an unflushed raw sample on it, returns what the merge that joins them
// stores: one rollup of the raw data.
func TestRerollReadFoldsRepresentatives(t *testing.T) {
	t.Parallel()

	for _, agg := range []signal.Aggregation{signal.AggSum, signal.AggAvg, signal.AggCount} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			opts := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: 6 * hr})

			r := newRerollEngine(t)
			r.write(rerollBase+min1, 1)
			r.write(rerollBase+2*min1, 9)
			r.flush()
			r.merge(opts)

			// A straddler is rewritten alone, so this representative lands in a part of its own.
			r.write(rerollBase-sec, 100)
			r.write(rerollBase+3*hr, 5)
			r.flush()
			r.merge(opts)
			require.Equal(t, 3, r.e.PartCount(), "two parts hold a representative at the bucket start")

			r.write(rerollBase, 7)

			r.assertOneRollup(opts.Downsample, 0)

			read, readVals := r.got()

			r.flush()

			opts.RetainFrom = rerollBase
			r.merge(opts)
			r.merge(opts)
			require.Equal(t, 1, r.e.PartCount())

			r.assertOneRollup(opts.Downsample, rerollBase)

			merged, mergedVals := r.got()
			assert.Equal(t, read[1:], merged, "the read before the merge")
			assert.Equal(t, readVals[1:], mergedVals, "the read before the merge")
		})
	}
}

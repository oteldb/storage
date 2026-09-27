package engine_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// rerollOp drives one step of [runRerollOracle].
type rerollOp uint8

const (
	opWrite rerollOp = iota
	opWriteOnStart
	opFlush
	opMergeFine
	opMergeCoarse
	opMergePlain
)

const rerollOps = uint8(opMergePlain) + 1

// runRerollOracle replays ops against one series: writes at distinct timestamps inside the first 17
// minutes of a day (some on minute starts, where Sum, Avg and Count representatives sit), flushes,
// and merges under a 1m tier, a 1m+5m policy, or none, in any order, so buckets are split across
// parts, rolled separately, coarsened, and joined by late samples. It then merges until the day is one
// part and requires one 5m rollup of every raw sample: exact but for Sum and Avg, which are held to
// the regrouping bound. A late write never reuses a timestamp: overwriting a rolled sample is #732.
func runRerollOracle(t *testing.T, agg signal.Aggregation, ops []byte) {
	t.Helper()

	fine := tiersOf(agg, engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: min1})
	coarse := tiersOf(agg,
		engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: min1},
		engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: 5 * min1})

	r := newRerollEngine(t)
	// The ladder leaves the newest day open; a sample two days on closes the one under test.
	r.write(rerollBase+2*dayNanos, 1)
	r.flush()

	written := map[int64]bool{}

	for i := 0; i+1 < len(ops); i += 2 {
		arg := int64(ops[i+1])

		switch rerollOp(ops[i] % rerollOps) {
		case opWrite, opWriteOnStart:
			ts := rerollBase + arg*4*sec
			if rerollOp(ops[i]%rerollOps) == opWriteOnStart {
				ts = rerollBase + (arg%17)*min1
			}

			if !written[ts] {
				written[ts] = true
				r.write(ts, float64(int8(ops[i+1]*37))*math.Pow10(int(arg%4)))
			}
		case opFlush:
			r.flush()
		case opMergeFine:
			r.merge(fine)
		case opMergeCoarse:
			r.merge(coarse)
		case opMergePlain:
			r.merge(engine.MergeOptions{})
		}
	}

	r.flush()

	settle := coarse
	settle.Force = true

	for range 16 {
		r.merge(settle)
	}

	wantParts := 1 // the anchor
	if len(r.raw) > 1 {
		wantParts++
	}

	require.Equal(t, wantParts, r.e.PartCount(), "the day under test compacts to one part")

	wantTs, wantVals := oneRollup(r.raw, coarse.Downsample, 0)

	got := fetchAll(t, r.e, fetch.Request{Start: -1 << 62, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Equal(t, wantTs, got[0].Timestamps, "timestamps")

	tol := 0.0
	if agg == signal.AggSum || agg == signal.AggAvg {
		vals := make([]float64, len(r.raw))
		for i, s := range r.raw {
			vals[i] = s.v
		}

		tol = engine.RegroupTolerance(vals)
	}

	population := map[int64]float64{}
	for _, s := range r.raw {
		if s.ts < rerollBase+dayNanos {
			population[s.ts-s.ts%(5*min1)]++
		}
	}

	for i, v := range got[0].Values {
		require.InDelta(t, wantVals[i], v, tol, "value at %d", got[0].Timestamps[i])

		wantW := 1.0
		if agg == signal.AggAvg && got[0].Timestamps[i] < rerollBase+dayNanos {
			wantW = population[got[0].Timestamps[i]]
		}

		require.InDelta(t, wantW, got[0].ScaleFactor(i), 0, "weight at %d", got[0].Timestamps[i])
	}
}

// TestRerollOracle runs [runRerollOracle] over fixed op sequences for every Agg.
func TestRerollOracle(t *testing.T) {
	t.Parallel()

	seqs := map[string][]byte{
		// Two parts each roll one half of a minute, then the halves meet.
		"SplitBucket": {0, 0, 0, 3, 2, 0, 3, 0, 0, 10, 0, 12, 2, 0, 3, 0, 5, 0},
		// A rolled minute takes a late sample, then a raw one on its start.
		"LateSamples": {0, 1, 0, 2, 0, 5, 2, 0, 3, 0, 0, 3, 2, 0, 1, 0, 2, 0, 3, 0},
		// Fine, then coarse, then a late sample into the coarse bucket.
		"CoarsenThenLate": {0, 1, 0, 20, 0, 40, 1, 2, 2, 0, 3, 0, 4, 0, 0, 60, 2, 0, 3, 0, 5, 0},
		// Plain compactions between rollups.
		"PlainBetween": {1, 0, 0, 7, 2, 0, 3, 0, 1, 1, 2, 0, 5, 0, 0, 30, 2, 0, 4, 0, 5, 0},
	}

	for _, agg := range []signal.Aggregation{
		signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg, signal.AggCount,
	} {
		for name, ops := range seqs {
			t.Run(agg.String()+"/"+name, func(t *testing.T) {
				t.Parallel()

				runRerollOracle(t, agg, ops)
			})
		}
	}
}

// FuzzRerollOracle is [TestRerollOracle] over fuzzed op sequences.
func FuzzRerollOracle(f *testing.F) {
	f.Add(uint8(4), []byte{0, 0, 0, 3, 2, 0, 3, 0, 0, 10, 0, 12, 2, 0, 3, 0, 5, 0})
	f.Add(uint8(6), []byte{0, 1, 0, 2, 0, 5, 2, 0, 3, 0, 0, 3, 2, 0, 1, 0, 2, 0, 3, 0})
	f.Add(uint8(5), []byte{0, 1, 0, 20, 0, 40, 1, 2, 2, 0, 3, 0, 4, 0, 0, 60, 2, 0, 3, 0, 5, 0})

	f.Fuzz(func(t *testing.T, aggByte uint8, ops []byte) {
		if len(ops) > 64 {
			ops = ops[:64]
		}

		runRerollOracle(t, signal.Aggregation(aggByte%7), ops)
	})
}

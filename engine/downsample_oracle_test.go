package engine_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
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
	opSwitchAgg
)

const rerollOps = uint8(opSwitchAgg) + 1

// oracleMode widens what [runRerollOracle] may do.
type oracleMode struct {
	// dups lets a write repeat one not yet merged, value included, as an exporter retry does, and
	// runs merges under a cap that rolls parts one at a time, so the two copies can be rolled apart.
	dups bool
	// mixedAggs lets the policy switch to another Agg between merges.
	mixedAggs bool
}

// runRerollOracle replays ops against one series: writes at distinct timestamps inside the first 17
// minutes of a day (some on minute starts, where Sum, Avg and Count representatives sit), flushes,
// and merges under a 1m tier, a 1m+5m policy, or none, in any order, so buckets are split across
// parts, rolled separately, coarsened, and joined by late samples. It then merges until the day is one
// part and requires one 5m rollup of every raw sample: exact but for Sum and Avg, which are held to
// the regrouping bound. A late write never reuses a timestamp: overwriting a rolled sample is #732.
//
// With duplicates the oracle keeps one copy. With mixed Aggs, a tier whose Agg differs from the one
// already recorded is not applied, so everything rolls by the first Agg a merge applied, and only as
// wide as a merge under that Agg reached.
func runRerollOracle(t *testing.T, agg signal.Aggregation, ops []byte, mode oracleMode) {
	t.Helper()

	fine := func(agg signal.Aggregation) engine.MergeOptions {
		return tiersOf(agg, engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: min1})
	}
	coarse := func(agg signal.Aggregation) engine.MergeOptions {
		return tiersOf(agg,
			engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: min1},
			engine.DownsampleTier{Before: rerollBase + dayNanos, Interval: 5 * min1})
	}

	var (
		recorded       signal.Aggregation
		hasRecorded    bool
		recordedCoarse bool
		flushedData    bool
	)

	// applied notes what a merge under agg with the given width records: the first Agg a merge rolls
	// data with is the only one ever applied.
	applied := func(agg signal.Aggregation, wide bool) {
		if !flushedData {
			return
		}

		if !hasRecorded {
			recorded, hasRecorded = agg, true
		}

		if agg == recorded && wide {
			recordedCoarse = true
		}
	}

	r := newRerollEngine(t)
	// The ladder leaves the newest day open; a sample two days on closes the one under test.
	r.write(rerollBase+2*dayNanos, 1)
	r.flush()

	var (
		written = map[int64]float64{}
		merged  = map[int64]bool{}
	)

	if mode.dups {
		r.e.SetMergeCeilingBytes(1)
	}

	mergeOp := func(opts engine.MergeOptions) {
		r.merge(opts)

		for ts := range written {
			merged[ts] = true
		}
	}

	for i := 0; i+1 < len(ops); i += 2 {
		arg := int64(ops[i+1])

		switch rerollOp(ops[i] % rerollOps) {
		case opWrite, opWriteOnStart:
			ts := rerollBase + arg*4*sec
			if rerollOp(ops[i]%rerollOps) == opWriteOnStart {
				ts = rerollBase + (arg%17)*min1
			}

			v, seen := written[ts]
			switch {
			case !seen:
				v = float64(int8(ops[i+1]*37)) * math.Pow10(int(arg%4))
				written[ts] = v
				r.write(ts, v)
			case mode.dups && !merged[ts]:
				r.write(ts, v)
			}
		case opFlush:
			r.flush()
			flushedData = flushedData || len(written) > 0
		case opMergeFine:
			applied(agg, false)
			mergeOp(fine(agg))
		case opMergeCoarse:
			applied(agg, true)
			mergeOp(coarse(agg))
		case opMergePlain:
			mergeOp(engine.MergeOptions{})
		case opSwitchAgg:
			if mode.mixedAggs {
				agg = signal.Aggregation(arg % 7)
			}
		}
	}

	r.flush()
	r.e.SetMergeCeilingBytes(0)

	flushedData = flushedData || len(written) > 0
	applied(agg, true)

	settle := coarse(agg)
	settle.Force = true

	if hasRecorded {
		agg = recorded
	}

	want := fine(agg)
	if recordedCoarse {
		want = coarse(agg)
	}

	width := want.Downsample[len(want.Downsample)-1].Interval

	for range 16 {
		r.merge(settle)
	}

	wantParts := 1 // the anchor
	if len(written) > 0 {
		wantParts++
	}

	require.Equal(t, wantParts, r.e.PartCount(), "the day under test compacts to one part")

	wantTs, wantVals := oneRollup(r.raw, want.Downsample, 0)

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
	for ts := range written {
		population[ts-ts%width]++
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

				runRerollOracle(t, agg, ops, oracleMode{})
			})
		}
	}
}

// TestRerollOracleMixedAggs is [TestRerollOracle] with the policy switching Agg between merges: the
// first Agg a merge applies is the only one ever applied, late data included, and a switch back to it
// lets its coarse tier apply again.
func TestRerollOracleMixedAggs(t *testing.T) {
	t.Parallel()

	seqs := map[string][]byte{
		// Roll fine, switch, write late, coarsen under the new Agg, then switch back and coarsen.
		"SwitchAndBack": {0, 1, 0, 2, 1, 3, 2, 0, 3, 0, 6, 6, 0, 20, 1, 1, 2, 0, 4, 0, 5, 0, 6, 4, 4, 0},
		// Switch before anything is rolled: the new Agg is the first applied.
		"SwitchFirst": {0, 1, 0, 30, 2, 0, 6, 2, 3, 0, 6, 5, 0, 31, 2, 0, 4, 0},
		// Coarsen first, then a different Agg meets late data in the coarse range.
		"CoarseThenSwitch": {0, 1, 0, 70, 2, 0, 4, 0, 6, 3, 0, 2, 2, 0, 3, 0, 4, 0},
	}

	for _, agg := range []signal.Aggregation{
		signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg, signal.AggCount,
	} {
		for name, ops := range seqs {
			t.Run(agg.String()+"/"+name, func(t *testing.T) {
				t.Parallel()

				runRerollOracle(t, agg, ops, oracleMode{mixedAggs: true})
			})
		}
	}
}

// FuzzRerollOracle is [TestRerollOracle] over fuzzed op sequences; an aggByte with its high bit set
// runs [TestRerollOracleMixedAggs]'s mode.
func FuzzRerollOracle(f *testing.F) {
	f.Add(uint8(4), []byte{0, 0, 0, 3, 2, 0, 3, 0, 0, 10, 0, 12, 2, 0, 3, 0, 5, 0})
	f.Add(uint8(6), []byte{0, 1, 0, 2, 0, 5, 2, 0, 3, 0, 0, 3, 2, 0, 1, 0, 2, 0, 3, 0})
	f.Add(uint8(5), []byte{0, 1, 0, 20, 0, 40, 1, 2, 2, 0, 3, 0, 4, 0, 0, 60, 2, 0, 3, 0, 5, 0})
	f.Add(uint8(0x84), []byte{0, 1, 0, 2, 1, 3, 2, 0, 3, 0, 6, 6, 0, 20, 1, 1, 2, 0, 4, 0, 5, 0, 6, 4, 4, 0})

	f.Fuzz(func(t *testing.T, aggByte uint8, ops []byte) {
		if len(ops) > 64 {
			ops = ops[:64]
		}

		runRerollOracle(t, signal.Aggregation(aggByte%7), ops, oracleMode{mixedAggs: aggByte&0x80 != 0})
	})
}

// rolledApartIssue gates the reproducers of copies of one sample rolled up by separate merges.
const rolledApartIssue = 739

// TestRerollOracleDuplicates is [TestRerollOracle] where a sample is written twice before either copy
// is merged, and the copies land in parts a capped merge rolls one at a time.
func TestRerollOracleDuplicates(t *testing.T) {
	reproduce.Unfixed(t, rolledApartIssue, "two representatives of copies of one sample, rolled apart, fold as two samples")
	t.Parallel()

	// Two copies of each sample in separate parts, then capped merges roll each part on its own.
	ops := []byte{0, 1, 0, 2, 1, 3, 2, 0, 0, 1, 0, 2, 1, 3, 2, 0, 3, 0, 3, 0, 3, 0, 3, 0}

	for _, agg := range []signal.Aggregation{
		signal.AggLast, signal.AggFirst, signal.AggMin, signal.AggMax, signal.AggSum, signal.AggAvg, signal.AggCount,
	} {
		t.Run(agg.String(), func(t *testing.T) {
			t.Parallel()

			runRerollOracle(t, agg, ops, oracleMode{dups: true})
		})
	}
}

// FuzzRerollOracleDuplicates is [TestRerollOracleDuplicates] over fuzzed op sequences.
func FuzzRerollOracleDuplicates(f *testing.F) {
	f.Add(uint8(4), []byte{0, 1, 0, 2, 1, 3, 2, 0, 0, 1, 0, 2, 1, 3, 2, 0, 3, 0, 3, 0, 3, 0, 3, 0})
	f.Add(uint8(6), []byte{0, 5, 2, 0, 0, 5, 2, 0, 3, 0, 3, 0})

	f.Fuzz(func(t *testing.T, aggByte uint8, ops []byte) {
		reproduce.Unfixed(t, rolledApartIssue, "two representatives of copies of one sample, rolled apart, fold as two samples")

		if len(ops) > 64 {
			ops = ops[:64]
		}

		runRerollOracle(t, signal.Aggregation(aggByte%7), ops, oracleMode{dups: true, mixedAggs: aggByte&0x80 != 0})
	})
}

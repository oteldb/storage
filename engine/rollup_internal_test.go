package engine

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

const (
	rollupPrefix = "default/metrics"
	// anchorAt is a part far enough ahead that the day holding the parts under test is not the
	// newest bucket, which the ladder leaves open; rolledBefore keeps it raw.
	anchorAt     = 5 * 24 * int64(time.Hour)
	rolledBefore = 2 * 24 * int64(time.Hour)
)

func rollupSeries() signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("job"), Value: signal.StringValue([]byte("api")),
	})}
}

// flushEvery appends one sample every step in [from, to) with value v, then flushes.
func flushEvery(t *testing.T, e *Engine, from, to, step int64, v float64) {
	t.Helper()

	s := rollupSeries()

	for ts := from; ts < to; ts += step {
		_, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{v}, []float64{1},
			func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
	}

	require.NoError(t, e.Flush(context.Background()))
}

func rollupSamples(t *testing.T, e *Engine) ([]int64, []float64) {
	t.Helper()

	ctx := context.Background()

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0].Timestamps, got[0].Values
}

func liveParts(e *Engine) []*part {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return slices.Clone(e.parts)
}

func forcedParts(e *Engine, opts MergeOptions) int {
	n := 0

	for _, p := range liveParts(e) {
		if forcedRewrite(p, opts) {
			n++
		}
	}

	return n
}

func backendKeys(t *testing.T, b backend.Backend) []string {
	t.Helper()

	keys, err := b.List(context.Background(), rollupPrefix)
	require.NoError(t, err)

	return keys
}

// stripRollups rewrites every part manifest under b without its marker, as a writer predating the
// marker left it.
func stripRollups(t *testing.T, b backend.Backend) {
	t.Helper()

	ctx := context.Background()

	for _, key := range backendKeys(t, b) {
		if !strings.HasSuffix(key, "/manifest") {
			continue
		}

		raw, err := b.Read(ctx, key)
		require.NoError(t, err)

		m, err := block.DecodeManifest(raw)
		if err != nil {
			continue
		}

		m.Rollup = nil
		require.NoError(t, b.Write(ctx, key, m.Encode(nil)))
	}
}

func reopenRollup(t *testing.T, b backend.Backend, cfg Config) *Engine {
	t.Helper()

	cfg.Backend, cfg.Prefix = b, rollupPrefix
	e := New(cfg)
	require.NoError(t, e.LoadParts(context.Background()))

	return e
}

func currentLayout(tiers []DownsampleTier) []DownsampleTier {
	m := rollupMarker(tiers)
	applied, _ := appliedRollup(&m)

	return applied
}

func TestRollupPending(t *testing.T) {
	t.Parallel()

	tier := func(before, interval int64, agg signal.Aggregation) DownsampleTier {
		return DownsampleTier{Before: before, Interval: interval, Agg: agg}
	}
	last := signal.AggLast
	twoTier := []DownsampleTier{tier(200, 1, last), tier(100, 5, last)}

	for _, tc := range []struct {
		name           string
		applied, tiers []DownsampleTier
		lo, hi         int64
		want           bool
	}{
		{"no downsampling", nil, nil, 0, 100, false},
		{"raw part past a cutoff", nil, []DownsampleTier{tier(100, 10, last)}, 50, 150, true},
		{"raw part younger than every cutoff", nil, []DownsampleTier{tier(100, 10, last)}, 100, 150, false},
		{"same layout across its cutoff", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(100, 10, last)}, 50, 150, false},
		{"frontier stepped inside the part", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(200, 10, last)}, 50, 150, true},
		{"frontier stepped past the part", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(200, 10, last)}, 150, 190, true},
		{"frontier stepped, part below both", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(200, 10, last)}, 20, 90, false},
		{"frontier stepped, part above both", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(200, 10, last)}, 200, 300, false},
		{"frontier stepped onto the part's newest", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(150, 10, last)}, 120, 150, true},
		{"multi-tier, same layout, spanning both cutoffs", twoTier, twoTier, 50, 250, false},
		{"multi-tier, order does not matter", twoTier, []DownsampleTier{twoTier[1], twoTier[0]}, 50, 250, false},
		{
			"multi-tier, coarse cutoff stepped inside the part", twoTier,
			[]DownsampleTier{tier(200, 1, last), tier(150, 5, last)}, 120, 180, true,
		},
		{
			"multi-tier, coarse cutoff stepped below the part", twoTier,
			[]DownsampleTier{tier(200, 1, last), tier(150, 5, last)}, 160, 180, false,
		},
		{"policy removed: rolled data stays rolled", []DownsampleTier{tier(100, 10, last)}, nil, 50, 60, false},
		{"tier removed: the wider recorded tier is kept", twoTier, []DownsampleTier{tier(200, 1, last)}, 50, 250, false},
		{"aggregation changed at the same width", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(100, 10, signal.AggMax)}, 50, 60, false},
		{"interval widened", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(100, 20, last)}, 50, 60, true},
		{"interval narrowed", []DownsampleTier{tier(100, 20, last)}, []DownsampleTier{tier(100, 10, last)}, 50, 60, false},
		{"tier re-added under a wider record", []DownsampleTier{tier(200, 5, last)}, twoTier, 50, 150, false},
		{"disabled tier is ignored", []DownsampleTier{tier(100, 10, last)}, []DownsampleTier{tier(100, 10, last), tier(500, 0, last)}, 50, 300, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, rollupPending(tc.applied, tc.tiers, tc.lo, tc.hi))
		})
	}
}

func TestDownsamplePendingUnknownFallsBack(t *testing.T) {
	t.Parallel()

	tiers := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggLast}}

	assert.True(t, downsamplePending(&part{minTime: 50, maxTime: 60}, tiers), "unknown and old: forced")
	assert.False(t, downsamplePending(&part{minTime: 100, maxTime: 160}, tiers), "unknown and young: not forced")
	assert.False(t, downsamplePending(&part{minTime: 50, maxTime: 60, rollupKnown: true, rollup: currentLayout(tiers)}, tiers))
	assert.True(t, downsamplePending(&part{minTime: 50, maxTime: 60, rollupKnown: true}, tiers), "known raw and old")
}

func TestRollupMarkerCanonical(t *testing.T) {
	t.Parallel()

	tiers := []DownsampleTier{
		{Before: 300, Interval: 60, Agg: signal.AggSum},
		{Before: 900, Interval: 0, Agg: signal.AggMax},
		{Before: 100, Interval: 3600, Agg: signal.AggCount},
	}

	assert.Equal(t, block.Rollup{Tiers: []block.RollupTier{
		{Before: 100, Interval: 3600, Agg: uint8(signal.AggCount)},
		{Before: 300, Interval: 60, Agg: uint8(signal.AggSum)},
	}}, rollupMarker(tiers))
	assert.Equal(t, block.Rollup{}, rollupMarker(nil))

	assert.Equal(t, []DownsampleTier{{Before: 200, Interval: 60, Agg: signal.AggSum}}, compactLayout([]DownsampleTier{
		{Before: 100, Interval: 60, Agg: signal.AggSum},
		{Before: 200, Interval: 60, Agg: signal.AggSum},
		{Before: 200, Interval: 60, Agg: signal.AggSum},
	}), "a later Before of the same tier covers an earlier one")

	mixed := []DownsampleTier{
		{Before: 100, Interval: 60, Agg: signal.AggSum},
		{Before: 150, Interval: 60, Agg: signal.AggMax},
		{Before: 200, Interval: 60, Agg: signal.AggSum},
	}
	assert.Len(t, compactLayout(mixed), 3, "a second Agg at the same Interval keeps every Before")
	assert.False(t, layoutsDiffer(mixed, compactLayout(mixed), 0, 300))

	_, known := appliedRollup(nil)
	assert.False(t, known)

	for lo := int64(0); lo < 1000; lo += 50 {
		assert.False(t, rollupPending(currentLayout(tiers), tiers, lo, lo+500))
	}
}

// TestDownsampleFrontierForcedOncePerStep checks a part straddling the cutoff is rewritten once each
// time the cutoff steps and not again, and that a restart does not force it anew.
func TestDownsampleFrontierForcedOncePerStep(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	e := reopenRollup(t, b, Config{})

	hour, minute := int64(time.Hour), int64(time.Minute)
	flushEvery(t, e, 0, 3*hour, 10*int64(time.Second), 1)

	opts := func(before int64) MergeOptions {
		return MergeOptions{Downsample: []DownsampleTier{{Before: before, Interval: minute, Agg: signal.AggLast}}}
	}

	for step, before := range []int64{hour, 2 * hour} {
		require.Equal(t, 1, forcedParts(e, opts(before)), "step %d", step)
		require.NoError(t, e.MergeWith(ctx, opts(before)))
		require.Zero(t, forcedParts(e, opts(before)), "step %d", step)

		keys := backendKeys(t, b)
		require.NoError(t, e.MergeWith(ctx, opts(before)))
		require.Equal(t, keys, backendKeys(t, b), "step %d: a second merge rewrites nothing", step)

		ts, _ := rollupSamples(t, e)
		require.Len(t, ts, int(before/minute+(3*hour-before)/(10*int64(time.Second))), "step %d", step)
	}

	r := reopenRollup(t, b, Config{})
	for _, p := range liveParts(r) {
		assert.True(t, p.rollupKnown)
	}

	assert.Zero(t, forcedParts(r, opts(2*hour)), "a restart reads the marker back")

	keys := backendKeys(t, b)
	require.NoError(t, r.MergeWith(ctx, opts(2*hour)))
	assert.Equal(t, keys, backendKeys(t, b))
}

// TestDownsampleLegacyPart checks a part written before the marker existed is rolled as raw data and
// carries the marker after one cycle, so it is not forced again. It is copied verbatim only where
// rolling it changes nothing; values on bucket starts are raw values, so a Count tier counts them.
func TestDownsampleLegacyPart(t *testing.T) {
	t.Parallel()

	minute := int64(time.Minute)

	for _, tc := range []struct {
		name      string
		agg       signal.Aggregation
		step      int64
		value     float64
		wantValue float64
		verbatim  bool
	}{
		{"on bucket starts, count", signal.AggCount, minute, 6, 1, false},
		{"on bucket starts, last", signal.AggLast, minute, 6, 6, true},
		{"raw, count", signal.AggCount, 10 * int64(time.Second), 1, 6, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tiers := []DownsampleTier{{Before: 1 << 62, Interval: minute, Agg: tc.agg}}
			opts := MergeOptions{Downsample: tiers}
			b := backend.Memory()
			flushEvery(t, reopenRollup(t, b, Config{}), 0, 10*minute, tc.step, tc.value)
			stripRollups(t, b)

			e := reopenRollup(t, b, Config{})
			parts := liveParts(e)
			require.Len(t, parts, 1)
			require.False(t, parts[0].rollupKnown)
			require.Equal(t, 1, forcedParts(e, opts))

			changes, err := e.rollupChanges(ctx, parts[0], minInt64, tiers)
			require.NoError(t, err)
			assert.Equal(t, !tc.verbatim, changes)

			require.NoError(t, e.MergeWith(ctx, opts))

			ts, vals := rollupSamples(t, e)
			require.Len(t, ts, 10)

			for i := range ts {
				if tc.agg == signal.AggCount {
					assert.Equal(t, int64(i)*minute, ts[i])
				}

				assert.InDelta(t, tc.wantValue, vals[i], 0)
			}

			parts = liveParts(e)
			require.Len(t, parts, 1)
			assert.True(t, parts[0].rollupKnown)
			assert.Equal(t, currentLayout(tiers), parts[0].rollup)
			assert.Zero(t, forcedParts(e, opts))
		})
	}
}

// TestDownsampleMarkedRawAlignedIsRolled checks a raw part whose samples already sit on bucket starts
// is still rolled when the rollup changes their values or weights: its marker says the values are raw,
// so a verbatim copy would record a layout it did not apply. An Avg over one sample keeps its value
// and weight, so there the copy is the rollup.
func TestDownsampleMarkedRawAlignedIsRolled(t *testing.T) {
	t.Parallel()

	minute := int64(time.Minute)

	for _, tc := range []struct {
		name         string
		agg          signal.Aggregation
		value, sf    float64
		want, wantSF float64
	}{
		{"count", signal.AggCount, 5, 1, 1, 1},
		{"weighted sum", signal.AggSum, 3, 2, 6, 1},
		{"weighted avg", signal.AggAvg, 3, 2, 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			e := reopenRollup(t, backend.Memory(), Config{})
			s := rollupSeries()

			for i := range int64(10) {
				_, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{i * minute}, []float64{tc.value},
					[]float64{tc.sf}, func(int) signal.Series { return s }, AppendLimits{})
				require.NoError(t, err)
			}

			require.NoError(t, e.Flush(ctx))

			parts := liveParts(e)
			require.Len(t, parts, 1)
			require.True(t, parts[0].rollupKnown)
			require.Empty(t, parts[0].rollup)

			tiers := []DownsampleTier{{Before: 1 << 62, Interval: minute, Agg: tc.agg}}
			require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))

			it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
			require.NoError(t, err)
			got, err := fetch.Drain(ctx, it)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Len(t, got[0].Timestamps, 10)

			for i := range got[0].Values {
				assert.InDelta(t, tc.want, got[0].Values[i], 0)

				assert.InDelta(t, tc.wantSF, got[0].ScaleFactor(i), 0, "the rollup's weight")
			}

			assert.Equal(t, currentLayout(tiers), liveParts(e)[0].rollup)
		})
	}
}

// TestDownsampleMarkedWithUnmarked checks a merge of a marked and an unmarked part records the
// current layout.
func TestDownsampleMarkedWithUnmarked(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	minute, second := int64(time.Minute), int64(time.Second)
	b := backend.Memory()

	flushEvery(t, reopenRollup(t, b, Config{}), 0, minute, second, 1)
	stripRollups(t, b)

	e := reopenRollup(t, b, Config{MergeCeilingBytes: -1})
	flushEvery(t, e, minute, 2*minute, second, 1)

	parts := liveParts(e)
	require.Len(t, parts, 2)
	require.NotEqual(t, parts[0].rollupKnown, parts[1].rollupKnown)

	tiers := []DownsampleTier{
		{Before: 1 << 62, Interval: minute, Agg: signal.AggSum},
		{Before: 1 << 61, Interval: 2 * minute, Agg: signal.AggSum},
	}
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))

	parts = liveParts(e)
	require.Len(t, parts, 1)
	assert.True(t, parts[0].rollupKnown)
	assert.Equal(t, currentLayout(tiers), parts[0].rollup)

	ts, vals := rollupSamples(t, e)
	assert.Equal(t, []int64{0}, ts)
	assert.Equal(t, []float64{120}, vals)
}

func countTiers(before int64, intervals ...time.Duration) []DownsampleTier {
	out := make([]DownsampleTier, 0, len(intervals))
	for _, iv := range intervals {
		out = append(out, DownsampleTier{Before: before, Interval: int64(iv), Agg: signal.AggCount})
	}

	return out
}

// rollTwoHours flushes two minutes of 10s samples at the start of hours 0 and 1 and rolls each part
// under tiers on its own, the older bucket first.
func rollTwoHours(t *testing.T, e *Engine, tiers []DownsampleTier) {
	t.Helper()

	ctx := context.Background()
	hour, minute := int64(time.Hour), int64(time.Minute)

	flushEvery(t, e, 0, 2*minute, 10*int64(time.Second), 1)
	flushEvery(t, e, hour, hour+2*minute, 10*int64(time.Second), 1)

	for range 2 {
		require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))
	}

	parts := liveParts(e)
	require.Len(t, parts, 2)

	for _, p := range parts {
		require.True(t, p.rollupKnown)
		require.Equal(t, currentLayout(tiers), p.rollup)
	}

	require.Zero(t, forcedParts(e, MergeOptions{Downsample: tiers}))

	flushEvery(t, e, anchorAt, anchorAt+1, 1, 1)
}

func samplesBefore(t *testing.T, e *Engine, end int64) ([]int64, []float64) {
	t.Helper()

	ts, vals := rollupSamples(t, e)
	n, _ := slices.BinarySearch(ts, end)

	return ts[:n], vals[:n]
}

// oldPart returns the part holding the oldest samples.
func oldPart(t *testing.T, e *Engine) *part {
	t.Helper()

	parts := liveParts(e)
	require.NotEmpty(t, parts)

	return slices.MinFunc(parts, func(a, b *part) int { return cmp.Compare(a.minTime, b.minTime) })
}

func requireCounts(t *testing.T, e *Engine, ts []int64, count float64) {
	t.Helper()

	gotTs, vals := samplesBefore(t, e, anchorAt)
	require.Equal(t, ts, gotTs)

	for _, v := range vals {
		require.InDelta(t, count, v, 0)
	}
}

// TestDownsampleLadderKeepsRolledCounts checks a ladder merge of parts already in the current layout
// does not re-roll their representatives, which would recount each Count bucket as 1.
func TestDownsampleLadderKeepsRolledCounts(t *testing.T) {
	t.Parallel()

	hour, minute := int64(time.Hour), int64(time.Minute)
	tiers := countTiers(rolledBefore, time.Minute)
	e := reopenRollup(t, backend.Memory(), Config{})
	rollTwoHours(t, e, tiers)

	require.NoError(t, e.MergeWith(context.Background(), MergeOptions{Downsample: tiers}))
	require.Equal(t, 2, e.PartCount(), "the ladder merged them")
	assert.Equal(t, currentLayout(tiers), oldPart(t, e).rollup)
	requireCounts(t, e, []int64{0, minute, hour, hour + minute}, 6)
}

// TestDownsampleEmptyTierMergeKeepsLayout checks a merge applying no tiers records the layout its
// sources already had rather than raw, so a later policy does not re-roll them.
func TestDownsampleEmptyTierMergeKeepsLayout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	hour, minute := int64(time.Hour), int64(time.Minute)
	tiers := countTiers(rolledBefore, time.Minute)
	b := backend.Memory()
	e := reopenRollup(t, b, Config{})
	rollTwoHours(t, e, tiers)

	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 2, e.PartCount())
	assert.Equal(t, currentLayout(tiers), oldPart(t, e).rollup)

	assert.Zero(t, forcedParts(e, MergeOptions{Downsample: tiers}))

	keys := backendKeys(t, b)
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))
	assert.Equal(t, keys, backendKeys(t, b))
	requireCounts(t, e, []int64{0, minute, hour, hour + minute}, 6)
}

// TestDownsampleTierRemovedThenReadded checks a part rolled under a tier the policy later drops keeps
// that tier in its layout through a merge, and is not re-rolled when the tier comes back.
func TestDownsampleTierRemovedThenReadded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	hour := int64(time.Hour)
	full := countTiers(rolledBefore, 5*time.Minute, time.Minute)
	loosened := countTiers(rolledBefore, time.Minute)
	e := reopenRollup(t, backend.Memory(), Config{})
	rollTwoHours(t, e, full)

	require.Zero(t, forcedParts(e, MergeOptions{Downsample: loosened}), "a narrower policy is not pending")
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: loosened}))
	require.Equal(t, 2, e.PartCount())
	assert.Equal(t, currentLayout(full), oldPart(t, e).rollup, "the dropped tier stays recorded")

	assert.Zero(t, forcedParts(e, MergeOptions{Downsample: full}))
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: full}))
	requireCounts(t, e, []int64{0, hour}, 12)
}

// TestDownsampleEmptyTierRolledWithRaw checks a merge of a rolled and a raw part applying no tiers
// records the rolled layout either way: raw samples past the rolled range stay raw, and raw samples
// inside it are rolled by the recorded layout, since removing a policy does not un-roll the range.
func TestDownsampleEmptyTierRolledWithRaw(t *testing.T) {
	t.Parallel()

	hour, minute, step := int64(time.Hour), int64(time.Minute), 10*int64(time.Second)
	tiers := countTiers(hour, time.Minute)

	for _, tc := range []struct {
		name   string
		rawAt  int64
		wantTs []int64
	}{
		{"raw past the rolled range", hour, append([]int64{0, minute}, stepsFrom(hour, 12, step)...)},
		{"raw inside the rolled range", 30 * minute, []int64{0, minute, 30 * minute, 31 * minute}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			e := reopenRollup(t, backend.Memory(), Config{})

			flushEvery(t, e, 0, 2*minute, step, 1)
			require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))
			flushEvery(t, e, tc.rawAt, tc.rawAt+2*minute, step, 1)
			flushEvery(t, e, anchorAt, anchorAt+1, step, 1)
			require.Equal(t, 3, e.PartCount())

			require.NoError(t, e.Merge(ctx, 0))
			require.Equal(t, 2, e.PartCount())

			p := oldPart(t, e)
			require.True(t, p.rollupKnown)
			assert.Equal(t, currentLayout(tiers), p.rollup)

			ts, vals := samplesBefore(t, e, anchorAt)
			assert.Equal(t, tc.wantTs, ts)

			for i := range ts {
				if ts[i] < hour {
					assert.InDelta(t, 6, vals[i], 0, "a count of the minute's samples")
				}
			}
		})
	}
}

func stepsFrom(from int64, n int, step int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = from + int64(i)*step
	}

	return out
}

// TestDownsampleCappedBucketRollsEveryPart checks that under a merge cap too small for a bucket's
// forced parts, each one is still rolled — a rolled output is not reselected ahead of the rest — and
// the ladder then compacts the bucket.
func TestDownsampleCappedBucketRollsEveryPart(t *testing.T) {
	t.Parallel()

	const parts = 8

	ctx := context.Background()
	minute := int64(time.Minute)
	e := reopenRollup(t, backend.Memory(), Config{})

	for i := range int64(parts) {
		flushEvery(t, e, i*minute, (i+1)*minute, int64(time.Second), float64(i))
	}

	raw := liveParts(e)[0].sizeBytes()
	e.cfg.MergeCeilingBytes = raw * 3 / 2

	opts := MergeOptions{Downsample: []DownsampleTier{{Before: 1 << 62, Interval: minute, Agg: signal.AggLast}}}
	for range 4 * parts {
		require.NoError(t, e.MergeWith(ctx, opts))
	}

	t.Logf("raw part %d bytes, cap %d, %d parts left", raw, e.cfg.MergeCeilingBytes, e.PartCount())

	assert.Zero(t, forcedParts(e, opts))

	ts, vals := rollupSamples(t, e)
	require.Len(t, ts, parts)

	for i := range ts {
		assert.Equal(t, int64(i+1)*minute-int64(time.Second), ts[i], "each minute's last sample")
		assert.InDelta(t, float64(i), vals[i], 0)
	}

	assert.Less(t, e.PartCount(), parts, "the ladder ran once the bucket was rolled")
}

// TestDownsampleLegacyBesideMarkedIsRaw pins the rule for a part without a marker merged beside
// others: its samples are raw to the fold, so a legacy Count representative counts as one sample. Only
// a writer predating the marker leaves a part unknown; merges of marked parts always record one.
func TestDownsampleLegacyBesideMarkedIsRaw(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	minute, step := int64(time.Minute), 10*int64(time.Second)
	tiers := countTiers(1<<62, time.Minute)
	b := backend.Memory()

	legacy := reopenRollup(t, b, Config{})
	flushEvery(t, legacy, 0, minute, step, 1)
	require.NoError(t, legacy.MergeWith(ctx, MergeOptions{Downsample: tiers}))
	stripRollups(t, b)

	e := reopenRollup(t, b, Config{})
	flushEvery(t, e, 5*step+step/2, 5*step+step/2+1, step, 1)

	parts := liveParts(e)
	require.Len(t, parts, 2)
	require.NotEqual(t, parts[0].rollupKnown, parts[1].rollupKnown)

	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))
	require.Equal(t, 1, e.PartCount())

	ts, vals := rollupSamples(t, e)
	assert.Equal(t, []int64{0}, ts)
	assert.Equal(t, []float64{2}, vals, "the legacy count of 6 is one sample, the late sample another")
	assert.True(t, liveParts(e)[0].rollupKnown)
}

// TestDownsampleUnnestedTierNotApplied checks a policy tier whose Interval does not nest with one a
// part already records is not applied: the part is not forced, and a late raw part in its range is
// rolled by the recorded layout, exactly.
func TestDownsampleUnnestedTierNotApplied(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	minute, step := int64(time.Minute), 10*int64(time.Second)
	recorded := countTiers(rolledBefore, 5*time.Minute)
	unnested := MergeOptions{Downsample: countTiers(rolledBefore, 7*time.Minute)}

	e := reopenRollup(t, backend.Memory(), Config{})
	flushEvery(t, e, 0, 10*minute, step, 1)
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: recorded}))
	require.Equal(t, currentLayout(recorded), oldPart(t, e).rollup)

	require.Equal(t, 1, forcedParts(e, unnested), "the unnested tier alone would widen it")
	require.Zero(t, e.MergeShapeWith(unnested).Candidates, "so the merge does not take it")

	kept, dropped := compatibleTiers(liveParts(e), unnested.Downsample)
	assert.Empty(t, kept)
	assert.Equal(t, unnested.Downsample, dropped)

	flushEvery(t, e, 2*minute+step/2, 3*minute, step, 1)
	flushEvery(t, e, anchorAt, anchorAt+1, step, 1)

	for range 4 {
		require.NoError(t, e.MergeWith(ctx, unnested))
	}

	require.Equal(t, 2, e.PartCount())
	assert.Equal(t, currentLayout(recorded), oldPart(t, e).rollup)

	ts, vals := samplesBefore(t, e, anchorAt)
	assert.Equal(t, []int64{0, 5 * minute}, ts)
	assert.Equal(t, []float64{36, 30}, vals)
}

// TestDownsampleLegacyAlignedRawIsCounted checks a part without a marker whose raw samples sit on
// bucket starts is rolled as raw data: its values are not taken for Count representatives, so a count
// tier counts each sample once, before and after a late sample joins the bucket.
func TestDownsampleLegacyAlignedRawIsCounted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	minute := int64(time.Minute)
	tiers := countTiers(1<<62, time.Minute)
	b := backend.Memory()

	flushEvery(t, reopenRollup(t, b, Config{}), 0, 3*minute, minute, 100)
	stripRollups(t, b)

	e := reopenRollup(t, b, Config{})
	require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))

	ts, vals := rollupSamples(t, e)
	assert.Equal(t, []int64{0, minute, 2 * minute}, ts)
	assert.Equal(t, []float64{1, 1, 1}, vals, "each raw sample counts once")

	flushEvery(t, e, 30*int64(time.Second), 30*int64(time.Second)+1, 1, 7)

	for range 2 {
		require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: tiers}))
	}

	ts, vals = rollupSamples(t, e)
	assert.Equal(t, []int64{0, minute, 2 * minute}, ts)
	assert.Equal(t, []float64{2, 1, 1}, vals, "the late sample is one more")
}

// rewriteRollup replaces the marker in part prefix's manifest, as a node running another policy
// would have written it.
func rewriteRollup(t *testing.T, b backend.Backend, prefix string, r block.Rollup) {
	t.Helper()

	ctx := context.Background()
	key := prefix + "/manifest"

	raw, err := b.Read(ctx, key)
	require.NoError(t, err)

	m, err := block.DecodeManifest(raw)
	require.NoError(t, err)

	m.Rollup = &r
	require.NoError(t, b.Write(ctx, key, m.Encode(nil)))
}

// TestDownsampleQuarantinesSecondAgg checks a part recording a second Agg, which the engine never
// writes beside the first but may adopt, is left out of every merge: it stays as written and reads
// the same, while the rest of the store still compacts and rolls up.
func TestDownsampleQuarantinesSecondAgg(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	day, minute, step := 2*int64(time.Hour), int64(time.Minute), 10*int64(time.Second)
	count := MergeOptions{Downsample: countTiers(1<<62, time.Minute)}
	b := backend.Memory()

	e := reopenRollup(t, b, Config{})
	flushEvery(t, e, 0, minute, step, 1)
	flushEvery(t, e, day, day+minute, step, 1)
	flushEvery(t, e, anchorAt, anchorAt+1, step, 1)

	for range 3 {
		require.NoError(t, e.MergeWith(ctx, count))
	}

	require.Equal(t, 3, e.PartCount())

	other := slices.MaxFunc(liveParts(e), func(a, b *part) int {
		return cmp.Compare(btoi(a.minTime == day), btoi(b.minTime == day))
	})
	require.Equal(t, day, other.minTime)
	rewriteRollup(t, b, other.prefix, rollupMarker(sumTiers(1<<62, time.Minute)))

	e = reopenRollup(t, b, Config{})
	before, beforeVals := samplesBefore(t, e, anchorAt)

	pool, quarantined := mergePool(liveParts(e))
	if assert.Len(t, quarantined, 1) {
		assert.Equal(t, other.prefix, quarantined[0].prefix)
	}

	assert.Len(t, pool, 2)

	flushEvery(t, e, 3*step+step/2, 3*step+step/2+1, 1, 1)

	for range 4 {
		require.NoError(t, e.MergeWith(ctx, MergeOptions{Downsample: count.Downsample, Force: true}))
	}

	var found bool

	for _, p := range liveParts(e) {
		found = found || p.prefix == other.prefix
	}

	assert.True(t, found, "the quarantined part is never merged")

	require.Equal(t, []int64{0, day}, before)
	require.Equal(t, []float64{6, 6}, beforeVals)

	ts, vals := samplesBefore(t, e, anchorAt)
	assert.Equal(t, before, ts)
	assert.Equal(t, []float64{7, 6}, vals, "the pool still rolls: the late sample counts into the Count part")
}

func sumTiers(before int64, intervals ...time.Duration) []DownsampleTier {
	out := countTiers(before, intervals...)
	for i := range out {
		out[i].Agg = signal.AggSum
	}

	return out
}

func btoi(b bool) int {
	if b {
		return 1
	}

	return 0
}

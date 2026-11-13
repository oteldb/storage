package engine

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

func TestRollupClosureClampsAtFirstOpenBucket(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		lo, hi, cutoff int64
	}{
		{"bucket overlap", 15, 18, 10}, {"outside source bucket", 20, 25, 100},
		{"same bucket before source", 0, 0, 0}, {"whole source blocked", -10, 30, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			selected := &part{prefix: "selected", minTime: 1, maxTime: 15}
			other := &part{prefix: "other", minTime: tc.lo, maxTime: tc.hi}
			tiers := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggSum}}
			closed := closedRollupTiers([]*part{selected}, []*part{selected, other}, tiers)
			if tc.cutoff <= selected.minTime {
				require.Empty(t, closed)
			} else {
				require.Len(t, closed, 1)
				assert.Equal(t, tc.cutoff, closed[0].Before)
			}
			assert.EqualValues(t, 100, tiers[0].Before, "the configured policy is immutable")
		})
	}
}

func TestRollupClosurePreservesFinePartsWhileCoarseningIsBlocked(t *testing.T) {
	t.Parallel()
	fine := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggSum}}
	coarse := []DownsampleTier{{Before: 100, Interval: 100, Agg: signal.AggSum}}
	parts := make([]*part, maxMergeParts+1)
	for i := range parts {
		parts[i] = &part{prefix: strconv.Itoa(i), minTime: 0, maxTime: 0, rollupKnown: true, rollup: fine}
	}
	selected, tiers, closure := closeRollupSelection(parts[:maxMergeParts], parts, parts, MergeOptions{Downsample: coarse}, maxMergeParts)
	require.True(t, closure)
	require.Len(t, selected, maxMergeParts)
	require.Nil(t, tiers, "plain compaction keeps the fine representatives until the coarse bucket fits")
	require.True(t, holdsLayout(selected, mergeLayout(selected, nil)))
}

func TestRollupClosureUnknownAndPendingMissingBounds(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"wanted", "pending", "adopted", "pending hole", "unopened"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory()})
			want := bucketindex.Want{Prefix: "missing"}
			switch kind {
			case "wanted":
				e.wants = []bucketindex.Want{want}
			case "pending":
				e.pendingWants = []bucketindex.Want{want}
			case "adopted":
				e.adoptedWants = []bucketindex.Want{want}
			case "pending hole":
				e.pendingHoles = []bucketindex.Want{want}
			case "unopened":
				e.foreign = []bucketindex.Entry{{Prefix: "missing", MinTime: 100, MaxTime: 105}}
			}
			p := &part{prefix: "source", minTime: 101, maxTime: 104}
			readable := append([]*part{p}, e.unopenedLayoutsLocked()...)
			selected, _, _ := closeRollupSelection([]*part{p}, []*part{p}, readable,
				MergeOptions{Downsample: []DownsampleTier{{Before: 1000, Interval: 10, Agg: signal.AggSum}}}, maxMergeParts)
			require.Empty(t, selected, "an unavailable possible duplicate prevents rolling this bucket")
		})
	}
}

func TestRollupClosureGuardRejectsNewOverlaps(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"local", "foreign", "want", "raw merge", "learned layout"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			p := &part{prefix: "source", minTime: 11, maxTime: 12}
			e := New(Config{Backend: backend.Memory()})
			e.parts = []*part{p}
			marker := &block.Rollup{Tiers: []block.RollupTier{{Before: 100, Interval: 10, Agg: uint8(signal.AggSum)}}}
			if kind == "raw merge" {
				marker = &block.Rollup{}
			}
			readable := []*part{p}
			if kind == "learned layout" {
				e.foreign = []bucketindex.Entry{{Prefix: "old unknown", MinTime: 1000, MaxTime: 1005}}
				readable = append(readable, e.unopenedLayoutsLocked()...)
			}
			guard := rollupClosureGuard([]*part{p}, readable, marker, e)
			if kind == "raw merge" {
				require.Nil(t, guard)
				return
			}
			require.NoError(t, guard())
			switch kind {
			case "foreign":
				e.foreign = []bucketindex.Entry{{Prefix: "new", MinTime: 19, MaxTime: 19}}
			case "learned layout":
				e.foreign[0].Rollup = &bucketindex.Rollup{Tiers: []bucketindex.RollupTier{{Before: 10000, Interval: 10, Agg: uint8(signal.AggCount)}}}
			case "want":
				e.pendingWants = []bucketindex.Want{{Prefix: "new"}}
			default:
				e.parts = append(e.parts, &part{prefix: "new", minTime: 19, maxTime: 19})
			}
			require.ErrorIs(t, guard(), errRollupConflict)
		})
	}
}

func TestRollupBucketBoundsSaturate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                     string
		lo, hi, before, interval, wantLo, wantHi int64
	}{
		{"negative", -1, -1, 100, 10, -10, -1},
		{"minimum", minInt64, minInt64, minInt64 + 1, 3, minInt64, minInt64 + 1},
		{"maximum", maxInt64 - 2, maxInt64, maxInt64, 10, maxInt64 - 7, maxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lo, hi, ok := rollupSpan(&part{minTime: tc.lo, maxTime: tc.hi}, DownsampleTier{Before: tc.before, Interval: tc.interval})
			require.True(t, ok)
			assert.Equal(t, tc.wantLo, lo)
			assert.Equal(t, tc.wantHi, hi)
		})
	}
}

func TestRollupClosureReducesMoreThanMaxPartsUnderTinyCap(t *testing.T) {
	t.Parallel()
	for _, chunks := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunks=%t", chunks), func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory(), Prefix: "closure", MergeCeilingBytes: 1, MetricSeriesChunks: chunks})
			s := rollupSeries()
			for i := range maxMergeParts + 1 {
				if i == maxMergeParts {
					_, err := e.Append(s, int64(i), float64(i))
					require.NoError(t, err)
				}
				_, err := e.Append(s, int64(i+1), float64(i+1))
				require.NoError(t, err)
				require.NoError(t, e.Flush(context.Background()))
			}
			opts := MergeOptions{Downsample: []DownsampleTier{{Before: 100, Interval: 100, Agg: signal.AggSum}}}
			for range 3 {
				changed, err := e.MergeStep(t.Context(), opts)
				require.NoError(t, err)
				if e.PartCount() > 1 {
					require.True(t, changed)
				}
			}
			require.Equal(t, 1, e.PartCount())
			ts, values := rollupSamples(t, e)
			require.Equal(t, []int64{0}, ts)
			require.Equal(t, []float64{float64((maxMergeParts + 1) * (maxMergeParts + 2) / 2)}, values)
		})
	}
}

func TestRollupClosurePreviouslyUnknownLayoutLoadedBeforePublish(t *testing.T) {
	t.Parallel()
	p := &part{prefix: "source", minTime: 11, maxTime: 12}
	unknown := &part{prefix: "old-foreign", minTime: 1000, maxTime: 1005}
	e := New(Config{Backend: backend.Memory()})
	e.parts = []*part{p}
	e.foreign = []bucketindex.Entry{{Prefix: unknown.prefix, MinTime: unknown.minTime, MaxTime: unknown.maxTime}}
	marker := &block.Rollup{Tiers: []block.RollupTier{{Before: 100, Interval: 10, Agg: uint8(signal.AggSum)}}}
	guard := rollupClosureGuard([]*part{p}, []*part{p, unknown}, marker, e)
	require.NoError(t, guard())
	loaded := &part{prefix: unknown.prefix, minTime: 1000, maxTime: 1005, rollupKnown: true,
		rollup: []DownsampleTier{{Before: 10000, Interval: 10, Agg: signal.AggCount}}}
	// adoptLoadLocked is the state transition LoadParts/RefreshReplica commits under e.mu.
	e.adoptLoadLocked(&indexLoad{ix: &bucketindex.Index{}, parts: []*part{p, loaded}})
	require.True(t, e.mergeSourcesLive([]*part{p}))
	require.ErrorIs(t, guard(), errRollupConflict)
}

func TestRollupClosureMixedSourcesMakePlainProgressAtTwoInputLimit(t *testing.T) {
	t.Parallel()
	fine := []DownsampleTier{{Before: 100, Interval: 10, Agg: signal.AggSum}}
	a := &part{diskBytes: 64, prefix: "fine", minTime: 0, maxTime: 0, rollupKnown: true, rollup: fine}
	b := &part{diskBytes: 64, prefix: "raw-b", minTime: 11, maxTime: 12, rollupKnown: true}
	c := &part{diskBytes: 64, prefix: "raw-c", minTime: 13, maxTime: 14, rollupKnown: true}
	parts := []*part{a, b, c}
	cohorts := mergeCohorts(parts, parts)
	opts := MergeOptions{Downsample: []DownsampleTier{{Before: 100, Interval: 100, Agg: signal.AggSum}}}
	selection := cohortRunPlanLimited(cohorts, parts, opts, 1, mergeIdleRounds, 0, 2)
	require.Len(t, selection.parts, 2, "raw b+c can compact without rolling while a blocks closure")
	require.Contains(t, selection.parts, b)
	require.Contains(t, selection.parts, c)
}

func TestRollupClosureBufferedBoundsLifecycle(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory()})
	s := rollupSeries()
	for _, ts := range []int64{11, -3, 7} {
		_, err := e.Append(s, ts, 1)
		require.NoError(t, err)
	}
	blockers := e.unopenedLayoutsLocked()
	require.Len(t, blockers, 1)
	require.EqualValues(t, -3, blockers[0].minTime)
	require.EqualValues(t, 11, blockers[0].maxTime)
	detached, bytes, since := e.head.detach()
	require.False(t, e.head.bufferedKnown)
	blockers = e.unopenedLayoutsLocked()
	require.Len(t, blockers, 1)
	require.Equal(t, "\x00frozen", blockers[0].prefix)
	require.EqualValues(t, -3, blockers[0].minTime)
	_, err := e.Append(s, 20, 1)
	require.NoError(t, err)
	require.Len(t, e.unopenedLayoutsLocked(), 2)
	e.head.reattach(detached, bytes, since)
	blockers = e.unopenedLayoutsLocked()
	require.Len(t, blockers, 1)
	require.EqualValues(t, -3, blockers[0].minTime)
	e.head.trimBelowCovered(map[signal.SeriesID]int64{s.Hash(): 20})
	require.Empty(t, e.unopenedLayoutsLocked())
	require.False(t, e.head.bufferedKnown)
	_, err = e.Append(s, 33, 1)
	require.NoError(t, err)
	require.EqualValues(t, 33, e.head.bufferedMin)
	require.NoError(t, e.Reset(t.Context()))
	require.Empty(t, e.unopenedLayoutsLocked())
	require.False(t, e.head.bufferedKnown)
}

func TestRollupClosureStagedBoundsOnlyAppearOnApply(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory()})
	s := rollupSeries()
	batch := newWALBatch()
	for _, ts := range []int64{11, -3, 7} {
		outcome := batch.admit(e.head, s.Hash(), ts, 1, 1, 0, AppendLimits{}, func() signal.Series { return s })
		require.Equal(t, admitted, outcome)
	}
	require.False(t, e.head.bufferedKnown, "a failed WAL before apply changes no bound")
	require.Empty(t, e.unopenedLayoutsLocked())
	batch.apply(e.head)
	require.True(t, e.head.bufferedKnown)
	require.EqualValues(t, -3, e.head.bufferedMin)
	require.Len(t, e.unopenedLayoutsLocked(), 1)
}

func TestRollupClosureGuardSeesOlderHeadAdmission(t *testing.T) {
	t.Parallel()
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("frozen=%t", frozen), func(t *testing.T) {
			t.Parallel()
			e := New(Config{Backend: backend.Memory()})
			s := rollupSeries()
			_, err := e.Append(s, 200, 1)
			require.NoError(t, err)
			if frozen {
				_, _, _ = e.head.detach()
			}
			p := &part{prefix: "source", minTime: 11, maxTime: 12}
			e.parts = []*part{p}
			readable := append([]*part{p}, e.unopenedLayoutsLocked()...)
			marker := &block.Rollup{Tiers: []block.RollupTier{{Before: 100, Interval: 10, Agg: uint8(signal.AggSum)}}}
			guard := rollupClosureGuard([]*part{p}, readable, marker, e)
			require.NoError(t, guard())
			_, err = e.Append(s, 19, 1)
			require.NoError(t, err)
			require.ErrorIs(t, guard(), errRollupConflict, "a preexisting head prefix cannot hide newly admitted old data")
		})
	}
}

func TestRollupClosureInactiveFragmentMarkerCannotRollAnotherRawSource(t *testing.T) {
	t.Parallel()
	a := &part{prefix: "inactive", diskBytes: 64, minTime: 50, maxTime: 59, rollupKnown: true, rollup: []DownsampleTier{{Before: 50, Interval: 10, Agg: signal.AggSum}}}
	b := &part{prefix: "b", diskBytes: 64, minTime: 0, maxTime: 9, rollupKnown: true}
	c := &part{prefix: "c", diskBytes: 64, minTime: 0, maxTime: 9, rollupKnown: true}
	parts := []*part{a, b, c}
	opts := MergeOptions{Downsample: []DownsampleTier{{Before: 100, Interval: 100, Agg: signal.AggSum}}}
	selection := cohortRunPlanLimited(mergeCohorts(parts, parts), parts, opts, 1, mergeIdleRounds, 0, 2)
	require.Len(t, selection.parts, 2)
	layout := mergeLayout(selection.parts, selection.tiers)
	require.True(t, holdsLayout(selection.parts, layout), "fallback compaction must not introduce new rollups")
}

func TestRollupClosureEffectiveMarkerProperty(t *testing.T) {
	t.Parallel()
	layouts := [][]DownsampleTier{
		nil,
		{{Before: 100, Interval: 10, Agg: signal.AggSum}},
		{{Before: 50, Interval: 10, Agg: signal.AggSum}},
		{{Before: 100, Interval: 100, Agg: signal.AggSum}},
	}
	spans := [][2]int64{{0, 9}, {50, 59}, {90, 110}, {0, 110}}
	for shape := range 4096 {
		n := shape
		parts := make([]*part, 3)
		for i := range parts {
			p := partOfSize(i, 64)
			span := spans[n%len(spans)]
			n /= len(spans)
			p.minTime, p.maxTime = span[0], span[1]
			p.rollup, p.rollupKnown = layouts[n%len(layouts)], true
			n /= len(layouts)
			parts[i] = p
		}
		opts := MergeOptions{Downsample: []DownsampleTier{{Before: 100, Interval: 100, Agg: signal.AggSum}}}
		s := cohortRunPlanLimited(mergeCohorts(parts, parts), parts, opts, 1, mergeIdleRounds, 0, 2)
		if len(s.parts) == 0 {
			continue
		}
		effective := mergeLayout(s.parts, s.tiers)
		if holdsLayout(s.parts, effective) {
			continue
		}
		require.Equalf(t, effective, closedRollupTiers(s.parts, parts, effective), "shape %d introduces an unclosed marker", shape)
	}
}

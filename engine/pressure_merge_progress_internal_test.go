package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestPressureMergeBacklogProgress(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "merge-progress"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.ResidentHeadBytes = 1 << 30
	e.cfg.MergeMemoryBytes = 128 << 20
	e.head.disk = newDiskHeadState()
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	want := make(map[signal.SeriesID]float64)
	generation := 0
	appendParts := func(n int) {
		for range n {
			generation++
			for i := range 1 + generation%11 {
				s := benchSeries(i)
				_, err := e.Append(s, 100, float64(generation))
				require.NoError(t, err)
				want[s.Hash()] = float64(generation)
			}
			require.NoError(t, e.Flush(t.Context()))
		}
	}
	check := func() {
		for _, reload := range []bool{false, true} {
			if reload {
				require.NoError(t, e.LoadParts(t.Context()))
			}
			it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
			require.NoError(t, err)
			batches, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			require.Len(t, batches, len(want))
			for _, batch := range batches {
				require.Equal(t, []float64{want[batch.Series.Hash()]}, batch.Values)
			}
		}
	}
	appendParts(maxMergeParts + 13)
	require.Greater(t, e.PartCount(), maxMergeParts)
	for attempt := 0; e.PartCount() > 1 && attempt < maxMergeParts; attempt++ {
		before := e.PartCount()
		changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
		require.NoError(t, err)
		require.True(t, changed, "finite overlapping backlog must not repeat an ordering veto")
		require.Less(t, e.PartCount(), before)
		plan := e.LastMergePlan()
		require.LessOrEqual(t, plan.Selected, maxMergeParts)
		require.LessOrEqual(t, plan.InputResidentBytes+plan.OpenBytes+plan.ScratchBytes+plan.MetadataBytes+plan.OutputFloorBytes, plan.AllowanceBytes)
		check()
	}
	require.Equal(t, 1, e.PartCount())
	for range 6 {
		appendParts(5)
		for attempt := 0; e.PartCount() > 1 && attempt < 2; attempt++ {
			before := e.PartCount()
			changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
			require.NoError(t, err)
			require.True(t, changed, "each five-part generation must receive compaction service")
			require.Less(t, e.PartCount(), before)
		}
		require.Equal(t, 1, e.PartCount())
		check()
	}
}

func TestPressureMergeMemoryKeepsNewestAnchor(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e.cfg.Backend, e.cfg.Prefix = files, "anchor"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.ResidentHeadBytes = 1 << 30
	e.head.disk = newDiskHeadState()
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	input := nodeExporterInputBatch(8192, 0)
	for i := range input.ts {
		input.ts[i] = 100
	}
	for range 16 {
		res, err := e.AppendBatch(input.ids, input.ts, input.values, nil,
			func(i int) signal.Series { return input.series[i] }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, len(input.ids), res.Accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, 16)
	for i, p := range e.parts {
		p.diskBytes = int64(i + 1)
	}
	opts := MergeOptions{Force: true, Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
	readable := slices.Clone(e.parts)
	cohorts := mergeCohorts(e.parts, readable)
	want := e.parts[len(e.parts)-7:]
	metadata, err := e.mergeMetadataWorkspace(t.Context(), want)
	require.NoError(t, err)
	bound, err := e.mergeInputBound(t.Context(), want, false)
	require.NoError(t, err)
	inputBytes := buildbudget.Add(bound.bytes(), buildbudget.Add(metadata, mergeSeriesScratchBound(want, nil)))
	limit := max(2*inputBytes, inputBytes+e.minimumMergeOutput(want, 0, opts, nil))
	selection, err := e.selectBoundedMergeWithAllowance(t.Context(), cohorts, readable, opts, 0, mergeIdleRounds, 0, limit)
	require.NoError(t, err)
	require.True(t, slices.Equal(want, selection.parts), "repricing keeps the newest seven inputs; selected %d", len(selection.parts))
	require.Same(t, e.parts[len(e.parts)-1], selection.orderAnchor)
	require.Greater(t, selection.plan.Attempts, 1)
	require.LessOrEqual(t, selection.inputBytes+selection.plan.OutputFloorBytes, limit)
	require.True(t, e.pressureMergeSelectionAllowed(selection.parts))
	// Watermark fallback repricing must retain the same chronological end as well.
	selection, err = e.selectMergeInputsWithAnchor(t.Context(), cohorts, readable, opts, 0, mergeIdleRounds, 0,
		limit, map[*part]bool{want[0]: true}, selection.orderAnchor)
	if err != nil {
		require.ErrorIs(t, err, errMergeInputMemory)
	} else {
		require.Contains(t, selection.parts, e.parts[len(e.parts)-1])
		require.True(t, e.pressureMergeSelectionAllowed(selection.parts))
		require.LessOrEqual(t, selection.inputBytes+selection.plan.OutputFloorBytes, limit)
	}
}

func TestPressureMergeSuffixLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		sizes    []int64
		capBytes int64
		idle     int
		want     bool
	}{
		{name: "small unequal", sizes: []int64{1, 10}, want: true},
		{name: "balanced", sizes: []int64{1 << 20, 1 << 20}, want: true},
		{name: "spread", sizes: []int64{1 << 20, 10 << 20}},
		{name: "idle spread", sizes: []int64{1 << 20, 10 << 20}, idle: mergeIdleRounds, want: true},
		{name: "output cap", sizes: []int64{1 << 20, 1 << 20}, capBytes: 1500 << 10},
		{name: "sealed anchor", sizes: []int64{1 << 20, 8 << 20}, capBytes: 10 << 20, idle: mergeIdleRounds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var parts []*part
			for _, size := range tc.sizes {
				parts = append(parts, &part{diskBytes: size})
			}
			require.Equal(t, tc.want, pressureMergeSuffixQualifies(parts, tc.capBytes, tc.idle))
		})
	}
}

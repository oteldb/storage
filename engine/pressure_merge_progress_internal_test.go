package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestPressureMergeBacklogProgress(t *testing.T) {
	reproduce.Unfixed(t, 661, "ordering veto repeatedly selects old inputs instead of a safe newest suffix")
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
		changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
		require.NoError(t, err)
		require.True(t, changed, "each five-part generation must receive compaction service")
		require.Equal(t, 1, e.PartCount())
		check()
	}
}

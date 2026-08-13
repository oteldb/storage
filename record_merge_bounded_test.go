package storage

import (
	"context"
	"runtime"
	"runtime/debug"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// TestRecordMergeBoundedWorkingSet is the regression for the record engine's unbounded merge working
// set. Before size-tiered compaction, recordengine.Merge compacted every flushed part into one part
// every cycle, re-materializing the whole cumulative dataset in RAM — RSS grew ∝ total volume ingested
// (the OOM bulk-loading real data). With a bounded MaxPartSize the merge now compacts only a bounded,
// similarly-sized tier group and seals large parts, so the per-merge working set plateaus instead of
// climbing with cumulative rows, while no records are lost.
//
// medianMiB is the typical per-merge allocation of a run of rounds.
//
// This was a peak (max, less the single largest round, to leave room for one round to have absorbed
// a GC). A peak is the wrong statistic here: a round that pays a second sync.Pool refill costs ~25
// MiB over a ~40 MiB merge, and how many rounds do so is pure GC timing — Linux shows none at all,
// while a Windows CI run produced one in the first half and three in the second, so trimming exactly
// one flipped the verdict. See #293.
//
// The median is immune to any minority of such rounds, and for the regression this test guards it is
// also strictly more sensitive: the pre-fix merge cost grew with the cumulative dataset, so *every*
// round in the second half grew, not an occasional one. Against a synthetic proportional regression
// the median separates the halves by 2.85x where the trimmed peak managed 2.09x.
func medianMiB(xs []float64) float64 {
	sorted := slices.Sorted(slices.Values(xs))

	if n := len(sorted); n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}

	return sorted[len(sorted)/2]
}

//nolint:paralleltest // measures process-global runtime.MemStats; a parallel test's allocations add noise.
func TestRecordMergeBoundedWorkingSet(t *testing.T) {
	ctx := context.Background()

	// Measure merge allocation with the collector off. Each round already forces a GC before its
	// window, which drains the engine's sync.Pools, so every merge pays exactly one pool refill. A GC
	// landing *inside* the window drains them again and charges a second refill (~25 MiB here) to that
	// round — noise that is pure GC timing, unrelated to the merge working set this asserts on. With
	// the collector off each round pays one refill and the measurement is deterministic; TotalAlloc
	// counts allocation volume, not live heap, so no real merge allocation is hidden.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	fb, err := backendfile.New(t.TempDir())
	require.NoError(t, err)

	// Small MaxPartSize so the seal threshold (mergeHeight × MaxPartSize) is reached within the test's
	// data — exactly what bounds a real backfill. Background loop disabled; the test drives flush/merge.
	s, err := Open(ctx, Options{},
		WithBackend(fb),
		WithFlushInterval(-1),
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{Limits: tenant.Limits{MaxPartSize: 1 << 20}} // 1 MiB parts
		})),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	const (
		services   = 8
		perService = 800 // 6.4k records/round
		rounds     = 24
	)

	var (
		logical int64
		allocs  [rounds]float64 // per-merge alloc, by round
	)

	t.Logf("%-6s %-8s %-16s %-16s", "round", "parts", "merge_alloc_MiB", "cumulative_rows")
	for round := range rounds {
		_, werr := s.WriteLogs(ctx, genLogRound(round, services, perService, &logical))
		require.NoError(t, werr)

		eng, ok := s.lookupLogEngine("default")
		require.True(t, ok)
		require.NoError(t, eng.Flush(ctx))

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		require.NoError(t, eng.Merge(ctx, 0))

		runtime.ReadMemStats(&after)
		allocs[round] = float64(after.TotalAlloc-before.TotalAlloc) / (1 << 20)

		t.Logf("%-6d %-8d %-16.1f %-16d", round, eng.PartCount(), allocs[round], int64((round+1)*services*perService))
	}

	// The defining property of the fix: a merge late in the run (far more cumulative data behind it)
	// costs no more than one early on. Pre-fix this ratio grew without bound (merge ∝ dataset).
	firstHalf, secondHalf := medianMiB(allocs[:rounds/2]), medianMiB(allocs[rounds/2:])
	assert.LessOrEqualf(t, secondHalf, firstHalf*1.5,
		"per-merge working set must not grow with cumulative rows (first-half peak %.1f MiB, second-half peak %.1f MiB, all rounds %.1f)",
		firstHalf, secondHalf, allocs)

	// Correctness: every ingested record is still queryable after all the tiered merges.
	it, err := s.LogFetcher("default").Fetch(ctx, fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	total := 0
	for _, b := range got {
		total += len(b.Timestamps)
	}
	assert.Equal(t, rounds*services*perService, total, "no records lost across tiered merges")
}

// TestMedianSeparatesRegressionFromGCNoise pins the statistic itself, so the flake fixed in #293
// cannot come back unnoticed and the sensitivity claim above is checkable without a Windows runner.
//
// The samples are real: `windows` is the per-round series from the CI run that failed, `linux` a
// local run of the same test. `regression` is synthetic — merge cost growing with the cumulative
// dataset, which is what this test exists to catch.
func TestMedianSeparatesRegressionFromGCNoise(t *testing.T) {
	t.Parallel()

	const rounds = 24

	windows := []float64{
		35.9, 39.8, 40.6, 40.5, 36.8, 40.0, 40.4, 65.4, 36.8, 40.0, 40.6, 40.7,
		62.0, 40.0, 40.7, 40.7, 37.1, 40.0, 65.5, 40.8, 37.1, 40.0, 40.7, 65.6,
	}
	linux := []float64{
		35.7, 39.7, 40.2, 40.4, 36.6, 39.9, 40.2, 40.4, 36.7, 39.8, 40.3, 40.5,
		36.9, 39.9, 40.5, 40.5, 36.9, 39.9, 40.5, 40.5, 36.9, 39.9, 40.5, 40.5,
	}

	regression := make([]float64, rounds)
	for i := range regression {
		regression[i] = 35.7 * float64(i+1) / 4 // merge cost ∝ cumulative rows
	}

	ratio := func(xs []float64) float64 {
		return medianMiB(xs[rounds/2:]) / medianMiB(xs[:rounds/2])
	}

	assert.Less(t, ratio(windows), 1.5,
		"GC noise (three ~25 MiB pool refills in one half) must not read as growth")
	assert.Less(t, ratio(linux), 1.5, "a clean run must pass comfortably")
	assert.Greater(t, ratio(regression), 1.5, "a merge that grows with the dataset must still fail")
}

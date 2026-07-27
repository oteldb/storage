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
// trimmedPeakMiB is the largest per-merge allocation of a run of rounds ignoring the single largest
// — the peak with room for one round to have absorbed a GC. The plain peak is what the merge working
// set must be judged by (the regression shows up as occasional huge merges over a normal floor, so a
// median or mean would miss it), but as a raw max one noisy round decides the whole verdict.
func trimmedPeakMiB(xs []float64) float64 {
	sorted := slices.Sorted(slices.Values(xs))

	return sorted[len(sorted)-2]
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
	//
	// The peak of each half, less its single largest round. Disabling the collector removes the common
	// case of a GC landing inside a measurement window, but not every case: one round in a run still
	// occasionally pays a second pool refill (~25 MiB over a ~40 MiB merge), which as a raw max decides
	// the whole half and fails the test on GC timing alone. Unbounded merges spike more than once per
	// half and by more than a refill, so trimming one round keeps the signal.
	firstHalf, secondHalf := trimmedPeakMiB(allocs[:rounds/2]), trimmedPeakMiB(allocs[rounds/2:])
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

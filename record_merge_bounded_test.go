package storage

import (
	"context"
	"runtime"
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
//nolint:paralleltest // measures process-global runtime.MemStats; a parallel test's allocations add noise.
func TestRecordMergeBoundedWorkingSet(t *testing.T) {
	ctx := context.Background()

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
		logical    int64
		firstHalf  float64 // peak per-merge alloc over the first half of the run
		secondHalf float64 // …and the second half — must not grow with cumulative rows
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
		allocMiB := float64(after.TotalAlloc-before.TotalAlloc) / (1 << 20)

		if round < rounds/2 {
			firstHalf = max(firstHalf, allocMiB)
		} else {
			secondHalf = max(secondHalf, allocMiB)
		}

		t.Logf("%-6d %-8d %-16.1f %-16d", round, eng.PartCount(), allocMiB, int64((round+1)*services*perService))
	}

	// The defining property of the fix: the largest merge in the second half (far more cumulative data)
	// is no bigger than in the first half. Pre-fix this ratio grew without bound (merge ∝ dataset).
	assert.LessOrEqualf(t, secondHalf, firstHalf*1.5,
		"per-merge working set must not grow with cumulative rows (first-half peak %.1f MiB, second-half peak %.1f MiB)",
		firstHalf, secondHalf)

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

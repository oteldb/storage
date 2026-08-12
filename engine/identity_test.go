package engine_test

import (
	"context"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
)

func TestIdentityBytesOutlivesFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/identity"})

	for i := range 100 {
		mustAppend(t, e, mkSeries("job", "api", "inst", strconv.Itoa(i)), int64(i), float64(i))
	}

	before := e.Stats()
	require.Positive(t, before.HeadBytes, "samples are buffered")
	require.Positive(t, before.IdentityBytes, "identity state is counted")

	require.NoError(t, e.Flush(ctx))

	after := e.Stats()
	assert.Zero(t, after.HeadBytes, "a flush drains the buffered samples")
	assert.Equal(t, before.IdentityBytes, after.IdentityBytes,
		"a flush drains no identity — it outlives the samples it named")
	assert.Equal(t, after.IdentityBytes, e.IdentityBytes())
}

func TestIdentityBytesResetToZero(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/identity-reset"})

	for i := range 100 {
		mustAppend(t, e, mkSeries("job", "api", "inst", strconv.Itoa(i)), int64(i), float64(i))
	}

	require.Positive(t, e.IdentityBytes())
	require.NoError(t, e.Reset(ctx))

	assert.Zero(t, e.Stats().Series)
	assert.Less(t, e.IdentityBytes(), int64(1<<12), "Reset is the one path that reclaims identity")
}

// TestIdentityBytesApproximatesHeap is the measurement behind the metering: a churned series set,
// flushed so that no sample remains buffered, leaves a heap footprint that HeadBytes reports as
// zero. IdentityBytes must account for it — the number an operator caps and alerts on, and the
// baseline any future identity prune is verified against.
//
//nolint:paralleltest // reads process-wide heap statistics; a parallel test would pollute them
func TestIdentityBytesApproximatesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100 MB and measures the heap")
	}

	const series = 200_000

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/identity-heap"})

	base := heapAlloc()

	for i := range series {
		mustAppend(t, e, mkSeries("job", "api", "region", "eu", "inst", strconv.Itoa(i)), int64(i), 1)
	}

	require.NoError(t, e.Flush(ctx))

	st := e.Stats()
	require.Zero(t, st.HeadBytes, "no sample is buffered — everything below is identity")
	require.EqualValues(t, series, st.Series)

	// Keep the engine (and so its identity state) reachable across the measurement.
	grew := heapAlloc() - base
	runtime.KeepAlive(e)

	reported := st.IdentityBytes
	t.Logf("series=%d reported=%d heap=%d (%d B/series reported, %d B/series measured)",
		series, reported, grew, reported/series, grew/series)

	// The estimate is structural (it ignores size-class rounding, and the measurement includes the
	// flushed part's own bytes), so it is checked as a band rather than an equality. It lands within
	// a few percent in practice; the band is wide enough to absorb an allocator or map-layout change
	// without going green on a genuinely broken counter.
	assert.Greater(t, reported, grew*3/4, "identity accounting must not under-report by >25 %")
	assert.Less(t, reported, grew*5/4, "identity accounting must not over-report by >25 %")
}

// heapAlloc returns the live heap after a collection, so two readings bracket what a workload
// retained rather than what it churned through.
func heapAlloc() int64 {
	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)

	return int64(ms.HeapAlloc)
}

package recordengine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

// flushParts writes n one-record parts, returning their prefixes.
func flushParts(t *testing.T, e *recordengine.Engine, n int) []string {
	t.Helper()
	ctx := context.Background()

	for i := range n {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: "p" + string(rune('1'+i))}))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, n)

	return parts
}

// admissionRecorder stands in for the process-wide merge pool, recording what each merge asked for
// and whether it said it could wait. busy declines every request that will not wait, which is what
// a background merge meets when the budget is fully committed; a waiting caller is admitted, as the
// real pool would once a holder released.
type admissionRecorder struct {
	busy bool

	mu       sync.Mutex
	asked    []int64
	waited   []bool
	released int
}

func (a *admissionRecorder) admit(_ context.Context, bytes int64, wait bool) (func(), bool, error) {
	a.mu.Lock()
	a.asked = append(a.asked, bytes)
	a.waited = append(a.waited, wait)
	busy := a.busy
	a.mu.Unlock()

	// Only a caller that declined to wait can be turned away; the real pool queues the others.
	if busy && !wait {
		return nil, false, nil
	}

	return func() {
		a.mu.Lock()
		a.released++
		a.mu.Unlock()
	}, true, nil
}

func (a *admissionRecorder) calls() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]int64(nil), a.asked...)
}

func (a *admissionRecorder) waits() []bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]bool(nil), a.waited...)
}

// TestMergeAdmissionIsTakenOnlyWhenThereIsWork mirrors the metric engine's rule: admission is taken
// after part selection, so an engine with nothing to compact never queues behind one that has work.
func TestMergeAdmissionIsTakenOnlyWhenThereIsWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := recordengine.New(recordengine.Config{
		Schema:           testSchema,
		Backend:          backend.Memory(),
		Prefix:           "t/recs",
		MaxPartBytes:     1 << 20,
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	require.NoError(t, e.Merge(ctx, 0))
	assert.Empty(t, rec.calls(), "a merge with no parts must not reserve memory")

	flushParts(t, e, 4)

	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))
	require.Len(t, rec.calls(), 1, "a merge with work reserves once")
	assert.Equal(t, 1, rec.released, "and hands the reservation back when it ends")

	require.NoError(t, e.Merge(ctx, 0))
	assert.Len(t, rec.calls(), 1, "a no-op merge after a real one reserves nothing")
}

// TestMergeAdmissionAsksForItsShare pins the amount: the resident allowance, undoubled — 256 MiB
// over a 16-core ceiling supports four merges, so one merge's share is 64 MiB.
func TestMergeAdmissionAsksForItsShare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := recordengine.New(recordengine.Config{
		Schema:           testSchema,
		Backend:          backend.Memory(),
		Prefix:           "t/recs",
		MaxPartBytes:     1 << 20,
		MergeMemoryBytes: 256 << 20,
		MergeConcurrency: func() int { return 16 },
		MergeAdmission:   rec.admit,
	})

	flushParts(t, e, 4)
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	require.Len(t, rec.calls(), 1)
	assert.Equal(t, int64(64<<20), rec.calls()[0])
}

// TestMergeWithoutAdmissionStillMerges keeps the callback optional, which is what every engine
// constructed without a facade relies on.
func TestMergeWithoutAdmissionStillMerges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	flushParts(t, e, 4)
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	assert.Less(t, len(e.PartPrefixes()), 4, "the parts were compacted")
}

// TestBackgroundMergeDefersWhenTheBudgetIsBusy mirrors the metric engine: the maintenance loop's own
// merge declines rather than parking the goroutine it shares with flush pressure, and retries next
// cycle. Waiting is the default; Background is what the loop opts into.
func TestBackgroundMergeDefersWhenTheBudgetIsBusy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{busy: true}

	e := recordengine.New(recordengine.Config{
		Schema:           testSchema,
		Backend:          backend.Memory(),
		Prefix:           "t/recs",
		MaxPartBytes:     1 << 20,
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	flushParts(t, e, 4)
	before := len(e.PartPrefixes())

	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true, Background: true}),
		"a declined merge is a deferral, not a failure")
	assert.Len(t, e.PartPrefixes(), before, "nothing was compacted")
	assert.Equal(t, []bool{false}, rec.waits(), "a background merge does not wait")
	assert.True(t, e.MergeDeferred(), "the deferral is remembered, so the facade can retry it first")

	rec.busy = false
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true, Background: true}))
	assert.Less(t, len(e.PartPrefixes()), before)
	assert.False(t, e.MergeDeferred(), "and is cleared once the merge is admitted")
}

// TestWaitingIsTheDefault is the polarity: Background is opt-in, so Admin.Compact, Admin.Retention,
// a test, or an embedder driving the engine gets a merge rather than a silent no-op.
func TestWaitingIsTheDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := recordengine.New(recordengine.Config{
		Schema:           testSchema,
		Backend:          backend.Memory(),
		Prefix:           "t/recs",
		MaxPartBytes:     1 << 20,
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	flushParts(t, e, 4)
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))

	assert.Equal(t, []bool{true}, rec.waits(), "a merge nobody marked Background waits for its budget")
}

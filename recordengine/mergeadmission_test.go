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

// admissionRecorder stands in for the process-wide merge pool, recording what each merge asked for
// and whether it said it could wait. busy makes every request decline, which is what a background
// merge meets when the budget is fully committed.
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

	if busy {
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

// TestBackgroundMergeDefersWhenTheBudgetIsBusy mirrors the metric engine: the background path
// declines rather than parking the facade's shared maintenance goroutine, and retries next cycle.
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

	require.NoError(t, e.Merge(ctx, 0), "a declined merge is a deferral, not a failure")
	assert.Len(t, e.PartPrefixes(), before, "nothing was compacted")
	assert.Equal(t, []bool{false}, rec.waits(), "a background merge does not wait")

	rec.busy = false
	require.NoError(t, e.MergeWith(ctx, recordengine.MergeOptions{Force: true}))
	assert.Less(t, len(e.PartPrefixes()), before)
	assert.Equal(t, []bool{false, true}, rec.waits(), "a forced merge may block for its budget")
}

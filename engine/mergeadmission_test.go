package engine_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

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

// flushTinyParts writes n parts of a few series each, so a merge has something to select.
func flushTinyParts(t *testing.T, e *engine.Engine, n int) {
	t.Helper()

	ctx := context.Background()

	const series = 8

	ser := make([]signal.Series, series)
	ids := make([]signal.SeriesID, series)

	for i := range series {
		ser[i] = mkSeries("__name__", "m", "instance", "host-"+strconv.Itoa(i))
		ids[i] = ser[i].Hash()
	}

	flushCorpus(t, ctx, e, ser, ids, 1, n,
		func(p, i, _ int) int64 { return int64(p*1000 + i) },
		func(p, _, _ int) float64 { return float64(p) })
}

// TestMergeAdmissionIsTakenOnlyWhenThereIsWork is the placement rule. Admission is deliberately
// taken after part selection, not on entry: a cycle with nothing to compact must not queue behind a
// merge that is running, or one busy engine would stall every other engine's maintenance pass.
func TestMergeAdmissionIsTakenOnlyWhenThereIsWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	// Nothing flushed at all: the merge selects nothing and must not reserve a byte.
	require.NoError(t, e.Merge(ctx, 0))
	assert.Empty(t, rec.calls(), "a merge with no parts must not reserve memory")

	flushTinyParts(t, e, 4)

	require.NoError(t, e.Merge(ctx, 0))
	require.Len(t, rec.calls(), 1, "a merge with work reserves once")

	// Merging again compacts nothing — one part cannot be merged with itself.
	require.NoError(t, e.Merge(ctx, 0))
	assert.Len(t, rec.calls(), 1, "a no-op merge after a real one reserves nothing")
}

// TestMergeAdmissionAsksForItsShare pins what a merge reserves: the resident allowance the cap was
// derived from, undoubled. Reserving anything else would make the pool's arithmetic and the cap's
// disagree, which is the class of bug the pool exists to end.
func TestMergeAdmissionAsksForItsShare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	// 256 MiB over a 16-core ceiling supports four merges, so one merge's resident share is 64 MiB.
	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 256 << 20,
		MergeConcurrency: func() int { return 16 },
		MergeAdmission:   rec.admit,
	})

	flushTinyParts(t, e, 4)
	require.NoError(t, e.Merge(ctx, 0))

	require.Len(t, rec.calls(), 1)
	assert.Equal(t, int64(64<<20), rec.calls()[0])
	assert.Equal(t, 1, rec.released, "the reservation is handed back when the merge ends")
}

// TestMergeWithoutAdmissionStillMerges keeps the callback optional: an embedder that installs no
// pool, and every test that constructs an engine directly, must merge exactly as before.
func TestMergeWithoutAdmissionStillMerges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "m", MergeMemoryBytes: 1 << 30})

	flushTinyParts(t, e, 4)
	require.NoError(t, e.Merge(ctx, 0))

	assert.Len(t, e.Parts(), 1)
}

// TestBackgroundMergeDefersWhenTheBudgetIsBusy is why admission is non-blocking for the maintenance
// loop. The facade runs it on one goroutine shared with size-triggered flushes, so a merge that
// parked waiting for memory would hold back the mechanism that gives memory back. It declines, the
// parts stay, and the next cycle retries.
func TestBackgroundMergeDefersWhenTheBudgetIsBusy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{busy: true}

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	flushTinyParts(t, e, 4)
	before := len(e.Parts())

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}),
		"a declined merge is a deferral, not a failure")
	assert.Len(t, e.Parts(), before, "nothing was compacted")
	assert.Equal(t, []bool{false}, rec.waits(), "a background merge does not wait")
	assert.True(t, e.MergeDeferred(), "the deferral is remembered, so the facade can retry it first")

	// The budget frees up; the same parts merge on the next cycle.
	rec.busy = false
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}))
	assert.Less(t, len(e.Parts()), before)
	assert.False(t, e.MergeDeferred(), "and is cleared once the merge is admitted")
}

// TestWaitingIsTheDefault is the polarity that matters: MergeOptions.Background is opt-in, set only
// by the maintenance loop. Every other caller — Admin.Compact, Admin.Retention, a test, an embedder
// driving the engine — must get a merge rather than a silent no-op, so it waits.
func TestWaitingIsTheDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	flushTinyParts(t, e, 4)

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, []bool{true}, rec.waits(), "a plain Merge waits for its budget")
}

// TestDeferralKeepsTheIdleWaiverLadder pins a trap: the metric engine escapes a merge fixed point by
// counting fruitless cycles and then waiving its write-amplification guard. A deferral selected a
// run and merely could not fund it, so it has not broken the fixed point — zeroing the count there
// would restart the climb every cycle and a starved engine could never escape.
func TestDeferralKeepsTheIdleWaiverLadder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rec := &admissionRecorder{}

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   rec.admit,
	})

	// One part: nothing to merge with, so the idle counter climbs and admission is never consulted.
	flushTinyParts(t, e, 1)

	for range 2 {
		require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}))
	}

	climbed := e.MergeShape().IdleRounds
	require.Positive(t, climbed, "the engine must have climbed the ladder for the test to mean anything")
	assert.Empty(t, rec.calls(), "a cycle that selects nothing never consults the budget")

	// Now there is a run to select, but no budget to fund it.
	rec.busy = true
	flushTinyParts(t, e, 3)
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}))

	require.Len(t, rec.calls(), 1, "the run was selected and admission was asked")
	assert.Equal(t, climbed, e.MergeShape().IdleRounds,
		"a deferral leaves the idle count where it was; only an admitted merge resets it")

	rec.busy = false
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}))
	assert.Zero(t, e.MergeShape().IdleRounds, "an admitted merge does reset it")
}

// TestNonBackgroundNeverDefers is the invariant the Background polarity exists to make
// unrepresentable: any caller that did not mark its merge as the maintenance loop's own must get a
// merge, not a silent nil. Before the polarity was inverted, Admin.Compact and Admin.Retention —
// neither of which sets Force, the discriminator at the time — returned nil having done nothing.
func TestNonBackgroundNeverDefers(t *testing.T) {
	t.Parallel()

	shapes := map[string]engine.MergeOptions{
		"zero value":      {},
		"forced":          {Force: true},
		"with retention":  {RetainFrom: 1},
		"forced + retain": {Force: true, RetainFrom: 1},
	}

	for name, opts := range shapes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			// busy would decline anything that asked not to wait; none of these may ask.
			rec := &admissionRecorder{busy: true}

			e := engine.New(engine.Config{
				Backend:          backend.Memory(),
				Prefix:           "m",
				MergeMemoryBytes: 1 << 30,
				MergeAdmission:   rec.admit,
			})

			flushTinyParts(t, e, 4)
			require.NoError(t, e.MergeWith(ctx, opts))

			require.Len(t, rec.waits(), 1)
			assert.True(t, rec.waits()[0], "a merge nobody marked Background must wait, not decline")
			assert.False(t, e.MergeDeferred(), "and must not be recorded as deferred")
		})
	}
}

// TestRefusedWaiterIsAnError closes the same invariant from the other side. Nothing in the callback
// signature stops an embedder from returning ok=false to a caller that said it would wait; if the
// engine took that as a deferral, a non-Background merge would return nil having done nothing —
// exactly the silent no-op the Background polarity exists to prevent.
func TestRefusedWaiterIsAnError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	refuse := func(context.Context, int64, bool) (func(), bool, error) { return nil, false, nil }

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "m",
		MergeMemoryBytes: 1 << 30,
		MergeAdmission:   refuse,
	})

	flushTinyParts(t, e, 4)

	require.Error(t, e.Merge(ctx, 0), "a refused waiter must surface, not vanish")

	// A Background merge asked not to wait, so the same answer is a legitimate deferral.
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Background: true}))
	assert.True(t, e.MergeDeferred())
}

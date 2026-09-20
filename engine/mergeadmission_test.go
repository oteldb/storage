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

// admissionRecorder stands in for the process-wide merge pool, recording what each merge asked for.
type admissionRecorder struct {
	mu       sync.Mutex
	asked    []int64
	released int
}

func (a *admissionRecorder) admit(_ context.Context, bytes int64) (func(), error) {
	a.mu.Lock()
	a.asked = append(a.asked, bytes)
	a.mu.Unlock()

	return func() {
		a.mu.Lock()
		a.released++
		a.mu.Unlock()
	}, nil
}

func (a *admissionRecorder) calls() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]int64(nil), a.asked...)
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

	for p := range n {
		batch := make([]signal.SeriesID, series)
		ts := make([]int64, series)
		vals := make([]float64, series)

		for i := range series {
			batch[i] = ids[i]
			ts[i] = int64(p*1000 + i)
			vals[i] = float64(p)
		}

		_, err := e.AppendBatch(batch, ts, vals, nil,
			func(i int) signal.Series { return ser[i] }, engine.AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}
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

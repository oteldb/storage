package scale_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/scale"
	"github.com/oteldb/storage/signal"
)

// splitFanOutCap mirrors the unexported cap in the package under test. A test that read the constant
// could not tell a broken bound from a huge one, so it is restated here deliberately.
const splitFanOutCap = 16

// gatedFetcher blocks every Fetch until gate is closed, tracking how many calls are in flight and the
// most that were ever in flight at once — so a bounded fan-out is observable without any timing
// assumption: with the gate held, the in-flight count rises to the cap and stays there.
type gatedFetcher struct {
	gate chan struct{}

	inflight atomic.Int64
	peak     atomic.Int64
	total    atomic.Int64
}

func (f *gatedFetcher) Fetch(_ context.Context, r fetch.Request) (fetch.Iterator, error) {
	n := f.inflight.Add(1)
	f.total.Add(1)

	for { // record the high-water mark
		peak := f.peak.Load()
		if n <= peak || f.peak.CompareAndSwap(peak, n) {
			break
		}
	}

	<-f.gate

	f.inflight.Add(-1)

	// One sample per sub-window, at the window's start, so the merged result is checkable.
	return fetch.NewSliceIterator([]*fetch.Batch{{
		ID:         signal.SeriesID{Lo: 1},
		Timestamps: []int64{r.Start},
		Values:     []float64{float64(r.Start)},
	}}), nil
}

// TestSplitBoundsConcurrency is the #214 acceptance: a split wide enough to produce far more
// sub-windows than the cap keeps only cap sub-fetches in flight. Each in-flight sub-fetch pins parts
// and reserves decode memory, so an unbounded split contends on the very budget meant to bound it.
func TestSplitBoundsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		interval = int64(time.Hour)
		windows  = 720 // a 30-day range at hourly interval
	)

	inner := &gatedFetcher{gate: make(chan struct{})}
	f := scale.SplitFetcher{Inner: inner, Interval: interval}

	type result struct {
		batches []*fetch.Batch
		err     error
	}

	done := make(chan result, 1)

	go func() {
		it, err := f.Fetch(context.Background(), fetch.Request{Start: 0, End: windows*interval - 1})
		if err != nil {
			done <- result{err: err}

			return
		}

		batches, err := fetch.Drain(context.Background(), it)
		done <- result{batches: batches, err: err}
	}()

	// Every sub-fetch blocks on the gate, so the in-flight count climbs to the cap and stops there.
	// Waiting for it to *reach* the cap also proves the fan-out is still concurrent, not serialized.
	require.Eventually(t, func() bool {
		return inner.inflight.Load() == splitFanOutCap
	}, 3*time.Second, time.Millisecond, "in-flight sub-fetches never settled at the cap")

	assert.LessOrEqualf(t, inner.peak.Load(), int64(splitFanOutCap),
		"a split of %d sub-windows ran more than %d sub-fetches at once", windows, splitFanOutCap)
	assert.Lessf(t, inner.total.Load(), int64(windows),
		"the whole split ran while the gate was held, so nothing was bounded")

	close(inner.gate)

	got := <-done
	require.NoError(t, got.err)
	require.Len(t, got.batches, 1, "one series")
	assert.Len(t, got.batches[0].Timestamps, windows, "every sub-window contributed its sample")
	assert.Equal(t, int64(0), inner.inflight.Load())
	assert.Equal(t, int64(windows), inner.total.Load(), "every sub-window was fetched exactly once")
}

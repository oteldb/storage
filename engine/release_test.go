package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// readAllRelease fetches every series, copies out each batch's values, and Releases it — the
// realistic consumer pattern that recycles result buffers.
func readAllRelease(t *testing.T, e *engine.Engine) map[signal.SeriesID][]float64 {
	t.Helper()
	ctx := context.Background()

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62, Recycle: true})
	require.NoError(t, err)

	out := make(map[signal.SeriesID][]float64)

	for {
		b, err := it.Next(ctx)
		if err != nil {
			break
		}

		vals := make([]float64, len(b.Values)) // copy out before Release
		copy(vals, b.Values)
		out[b.ID] = vals
		b.Release()
	}

	require.NoError(t, it.Close())

	return out
}

// TestFetchReleaseRecycle proves recycling result buffers via Batch.Release is correct: a second
// fetch (which reuses the pooled buffers a prior release returned) yields identical data.
func TestFetchReleaseRecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	ids, series, ts, vals := distinctSeries(12)
	_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx)) // a second flush so a series can span parts on merge
	_, err = e.AppendBatch(ids[:6], ts[:6], vals[:6], nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)

	first := readAllRelease(t, e)  // populates the pool on release
	second := readAllRelease(t, e) // reuses pooled buffers
	third := readAllRelease(t, e)  // …again

	require.Len(t, first, 12)
	assert.Equal(t, first, second, "reused buffers must yield identical data")
	assert.Equal(t, first, third)
}

// TestFetchReleaseConcurrent exercises the release path under concurrency (run with -race): many
// goroutines fetch + release against one engine; the shared pool must never hand a buffer to two
// live batches.
func TestFetchReleaseConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	ids, series, ts, vals := distinctSeries(20)
	_, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i] }, engine.AppendLimits{})
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))

	want := len(readAllRelease(t, e))

	// fetchReleaseCount fetches every series and releases each batch, returning the count — no
	// testify (require/assert must stay on the test goroutine).
	fetchReleaseCount := func() int {
		it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62, Recycle: true})
		if err != nil {
			return -1
		}

		n := 0

		for {
			b, berr := it.Next(ctx)
			if berr != nil {
				break
			}

			n++

			b.Release()
		}

		_ = it.Close()

		return n
	}

	counts := make([]int, 8)

	var wg sync.WaitGroup
	for i := range counts {
		wg.Go(func() {
			for range 50 {
				counts[i] = fetchReleaseCount()
			}
		})
	}

	wg.Wait()

	for i, got := range counts {
		assert.Equalf(t, want, got, "goroutine %d series count", i)
	}
}

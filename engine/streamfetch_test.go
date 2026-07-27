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
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// seriesEngine returns an engine holding n distinct series with samplesPerSeries samples each,
// flushed into one part (so a fetch's samples come from decode, not the head snapshot).
func seriesEngine(ctx context.Context, tb testing.TB, n, samplesPerSeries int) *engine.Engine {
	tb.Helper()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	series := make([]signal.Series, n)
	for i := range series {
		series[i] = mkSeries("id", strconv.Itoa(i))
	}

	ids := make([]signal.SeriesID, 0, n)
	ts := make([]int64, 0, n)
	vals := make([]float64, 0, n)
	at := func(i int) signal.Series { return series[i%n] }

	for s := range samplesPerSeries {
		ids, ts, vals = ids[:0], ts[:0], vals[:0]

		for i := range series {
			ids = append(ids, series[i].Hash())
			ts = append(ts, int64(s+1))
			vals = append(vals, float64(i))
		}

		_, err := e.AppendBatch(ids, ts, vals, nil, at, engine.AppendLimits{})
		require.NoError(tb, err)
	}

	require.NoError(tb, e.Flush(ctx))

	return e
}

// TestFetchIsStreaming is the memory property of the streaming fetch: a consumer that reads and
// releases one batch at a time never has more than a couple of result buffers alive, however many
// series matched. Buffer identity is the observable — the recycling pool can only hand the next
// series the buffer we just released if that buffer is genuinely dead, which it is only because the
// batches are built one Next at a time.
func TestFetchIsStreaming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const series = 128

	e := seriesEngine(ctx, t, series, 4)

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62, Recycle: true})
	require.NoError(t, err)

	buffers := make(map[*int64]struct{})
	got := 0

	for {
		b, err := it.Next(ctx)
		if err != nil {
			break
		}

		require.NotEmpty(t, b.Timestamps)
		buffers[&b.Timestamps[0]] = struct{}{}
		got++

		b.Release() // the realistic consumer: fold, then hand the buffers back
	}

	require.NoError(t, it.Close())
	require.Equal(t, series, got, "every matched series is delivered")
	assert.Lessf(t, len(buffers), 8,
		"%d series were served from %d distinct result buffers — the fetch materialized them all",
		got, len(buffers))
}

// TestFetchIsStreamingWithoutRecycle checks the same iteration is correct on the default
// (non-pooling) path, and that abandoning it early is clean: Close settles the fetch whether or not
// the consumer read to the end.
func TestFetchIsStreamingWithoutRecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := seriesEngine(ctx, t, 32, 3)

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)

	b, err := it.Next(ctx)
	require.NoError(t, err)
	assert.Len(t, b.Timestamps, 3)

	require.NoError(t, it.Close(), "an abandoned fetch closes cleanly")
	require.NoError(t, it.Close(), "Close is idempotent")

	// The abandoned fetch released its parts and decode budget, so a full fetch still works.
	all, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, all)
	require.NoError(t, err)
	assert.Len(t, batches, 32)
}

// BenchmarkFetchResident reports the live heap a fetch holds at its peak, for two consumers over
// the same data: `stream` folds and releases one batch at a time (what the fetch contract promises),
// `drain` materializes the whole result set first (what the fetch itself used to do internally). The
// gap between them is what streaming the produce loop buys — and it grows with the matched-series
// count.
//
// `stream` is not flat in the series count: only the *batches* are streamed. The plan still holds
// one identity per matched series and a part still decodes at whole-column granularity, both O(the
// part's rows) — the block-sliced decode path narrows the latter, not this change.
func BenchmarkFetchResident(b *testing.B) {
	ctx := context.Background()

	// Each consumer reads the iterator and returns the fold (so nothing is optimized away) plus the
	// live heap measured *while it holds its working set*: mid-iteration for stream (one batch),
	// after the drain for drain (every batch). liveHeap forces a GC, so what it reports is resident
	// data, not floating garbage — at the cost of making ns/op a lower bound only.
	consumers := []struct {
		name string
		read func(b *testing.B, it fetch.Iterator, series int) (sum float64, live uint64)
	}{
		{"stream", func(b *testing.B, it fetch.Iterator, series int) (float64, uint64) {
			b.Helper()

			var (
				sum  float64
				live uint64
				seen int
			)

			for {
				batch, err := it.Next(ctx)
				if err != nil {
					return sum, live
				}

				for _, v := range batch.Values { // fold, hold nothing
					sum += v
				}

				seen++
				if seen == series/2 { // measure while a batch is in hand
					live = liveHeap()
				}

				batch.Release()
			}
		}},
		{"drain", func(b *testing.B, it fetch.Iterator, _ int) (float64, uint64) {
			b.Helper()

			batches, err := fetch.Drain(ctx, it)
			if err != nil {
				b.Fatal(err)
			}

			live := liveHeap()

			var sum float64

			for _, batch := range batches {
				for _, v := range batch.Values {
					sum += v
				}
			}

			return sum, live
		}},
	}

	// Two shapes: the recording-rule shape (very many series, few samples each) and the range-query
	// shape (fewer series, a long window each).
	shapes := []struct{ series, samples int }{
		{1_000, 8}, {10_000, 8}, {100_000, 8},
		{1_000, 500}, {10_000, 500},
	}

	for _, s := range shapes {
		for _, c := range consumers {
			name := strconv.Itoa(s.series) + "series_" + strconv.Itoa(s.samples) + "samples/" + c.name

			b.Run(name, func(b *testing.B) {
				e := seriesEngine(ctx, b, s.series, s.samples)
				req := fetch.Request{Start: 0, End: 1 << 62, Recycle: true}

				var peak uint64

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					it, err := e.Fetch(ctx, req)
					if err != nil {
						b.Fatal(err)
					}

					base := liveHeap()
					sum, live := c.read(b, it, s.series)

					if live > base && live-base > peak {
						peak = live - base
					}

					if err := it.Close(); err != nil {
						b.Fatal(err)
					}

					if sum == 0 {
						b.Fatal("no samples read")
					}
				}

				b.ReportMetric(float64(peak)/1024, "peak_KB")
			})
		}
	}
}

// liveHeap is the heap that survives a collection — the resident set at this instant.
func liveHeap() uint64 {
	var ms runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

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
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestBlockCacheEvictionIntegrity is the safety net for recycling evicted decode buffers under memory
// pressure. It pins the cache far below the working set so a broad fetch evicts its own just-decoded
// blocks constantly, then runs many such fetches concurrently and checks every series' samples come
// back exactly right. If a byte-budget eviction ever recycled a buffer a concurrent fetch still held a
// view of, the reused buffer would corrupt that fetch's result (and -race would flag the read) — so a
// green run under -race is the evidence recycling is sound.
func TestBlockCacheEvictionIntegrity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "t/evict",
		DecodeCacheBytes: 128, // far below the working set: constant eviction mid-fetch
		MetricBlockRows:  4,   // samples span several blocks per series
	})

	const series, samples = 48, 20

	want := make(map[signal.SeriesID]struct {
		ts   []int64
		vals []float64
	}, series)

	for s := range series {
		ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))

		ts := make([]int64, 0, samples)
		vals := make([]float64, 0, samples)

		for k := range samples {
			at := int64(1000 + s*10000 + k*10)
			v := float64(s*100 + k)
			mustAppend(t, e, ser, at, v)
			ts = append(ts, at)
			vals = append(vals, v)
		}

		want[ser.Hash()] = struct {
			ts   []int64
			vals []float64
		}{ts, vals}
	}

	require.NoError(t, e.Flush(ctx))

	all := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}}

	check := func(t *testing.T, batches []*fetch.Batch) {
		t.Helper()
		require.Len(t, batches, series)

		for _, b := range batches {
			w, ok := want[b.ID]
			require.True(t, ok, "unexpected series in result")
			assert.Equal(t, w.ts, b.Timestamps, "series %v timestamps intact under eviction", b.ID)
			assert.Equal(t, w.vals, b.Values, "series %v values intact under eviction", b.ID)
		}
	}

	// Single fetch: correctness with heavy eviction but no concurrency.
	check(t, fetchAll(t, e, all))

	// Concurrent broad fetches: each evicts blocks the others are decoding/viewing. Under -race this
	// catches any recycle-while-viewed; the value assertions catch corruption regardless of timing.
	// Fetch off the test goroutine but validate on it (require must not run from a goroutine).
	const readers = 16

	results := make([][]*fetch.Batch, readers)
	errs := make([]error, readers)

	var wg sync.WaitGroup
	for i := range readers {
		wg.Go(func() {
			it, err := e.Fetch(ctx, all)
			if err != nil {
				errs[i] = err

				return
			}

			results[i], errs[i] = fetch.Drain(ctx, it)
		})
	}

	wg.Wait()

	for i := range readers {
		require.NoError(t, errs[i])
		check(t, results[i])
	}

	// The cache did its job under pressure: it stayed within budget and still served hits.
	st, ok := e.DecodeCacheStats()
	require.True(t, ok)
	assert.LessOrEqual(t, st.Bytes, int64(128), "cache honored its byte budget")
	assert.Positive(t, st.Misses, "the tiny cache forced re-decodes")
}

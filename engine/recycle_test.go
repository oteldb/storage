package engine_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestDecodeRecycleAfterReclaim pins the decode-buffer recycle path: when a merge retires the source
// parts, reclaim evicts their decode-cache entries and returns the column buffers to the pool, so the
// next decode reuses that capacity. The contract under test is correctness — a re-fetch after the
// recycle must return intact columns (a recycled buffer must not corrupt or alias live data).
func TestDecodeRecycleAfterReclaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/recycle", DecodeCacheBytes: 1 << 20})

	s := mkSeries("job", "api")
	for i, ts := range []int64{100, 200, 300, 400} {
		mustAppend(t, e, s, ts, float64(i+1))
		require.NoError(t, e.Flush(ctx))
	}

	require.Equal(t, 4, e.PartCount())

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	// Populate the decode cache with the source parts' columns.
	got := fetchAll(t, e, req)
	require.Len(t, got, 1)
	require.Equal(t, []int64{100, 200, 300, 400}, got[0].Timestamps)

	// Merge retires the source parts; reclaim evicts their cache entries and recycles the buffers.
	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, 1, e.PartCount(), "four parts compact to one")

	// The next fetch decodes the merged part into pool-recycled buffers; results must be intact.
	got2 := fetchAll(t, e, req)
	require.Len(t, got2, 1)
	assert.Equal(t, []int64{100, 200, 300, 400}, got2[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3, 4}, got2[0].Values)
}

// BenchmarkDecodeRecycle drives the steady-state churn the recycle targets: each iteration flushes a
// fresh part, fetches it (a cache-miss decode that reuses a reclaimed part's recycled buffers), then
// merges (retiring + recycling the source parts' columns). ReportAllocs shows the decode-buffer
// allocation the recycle removes from this cycle (merge allocations are the shared baseline on both
// sides of an A/B).
func BenchmarkDecodeRecycle(b *testing.B) {
	ctx := context.Background()

	const series, samples = 400, 60

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "bench/recycle", DecodeCacheBytes: 256 << 20})
	ser, ids := buildNamedSeries(series, "node_x")
	flushParts(b, ctx, e, ser, ids, samples, 15, 1) // seed a part to merge against

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("__name__", "node_x")}}

	batchIDs := make([]signal.SeriesID, series*samples)
	ts := make([]int64, series*samples)
	vals := make([]float64, series*samples)
	resolve := func(i int) signal.Series { return ser[i/samples] }

	b.ReportAllocs()
	b.ResetTimer()

	for it := range b.N {
		k := 0

		for i := range ids {
			for s := range samples {
				batchIDs[k] = ids[i]
				ts[k] = int64((it+1)*100000+s*15) + int64(i)
				vals[k] = float64(i)
				k++
			}
		}

		if _, err := e.AppendBatch(batchIDs, ts, vals, nil, resolve, engine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}

		if err := e.Flush(ctx); err != nil {
			b.Fatal(err)
		}

		if _, err := fetch.Drain(ctx, mustFetch(b, e, req)); err != nil {
			b.Fatal(err)
		}

		if err := e.Merge(ctx, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func mustFetch(b *testing.B, e *engine.Engine, req fetch.Request) fetch.Iterator {
	b.Helper()

	it, err := e.Fetch(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}

	return it
}

// TestDecodeRecycleConcurrent stresses the refs-gated safety of the recycle: readers fetch in a tight
// loop while merges continuously retire and reclaim parts. A buffer recycled while a fetch still reads
// it would corrupt that fetch's result (or trip the race detector); the refs==0 reclaim gate must
// prevent it. Run under `go test -race`.
func TestDecodeRecycleConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/recycle-conc", DecodeCacheBytes: 1 << 20})

	const series = 64

	// Seed two parts so the first merge has something to compact.
	for round := range 2 {
		for i := range series {
			mustAppend(t, e, mkSeries("job", "api", "shard", strconv.Itoa(i)), int64(round*1000+i), float64(i))
		}

		require.NoError(t, e.Flush(ctx))
	}

	req := fetch.Request{Start: 0, End: 1 << 30, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}

	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		readErr atomic.Pointer[error]
	)

	for range 8 {
		wg.Go(func() {
			for !stop.Load() {
				it, err := e.Fetch(ctx, req)
				if err != nil {
					readErr.Store(&err)

					return
				}

				batches, err := fetch.Drain(ctx, it)
				if err != nil {
					readErr.Store(&err)

					return
				}

				if len(batches) != series {
					mismatch := assert.AnError
					readErr.Store(&mismatch)

					return
				}
			}
		})
	}

	// Drive retire/reclaim: each round adds a part then merges everything, retiring the sources whose
	// decoded columns get recycled while the readers above are mid-fetch.
	for round := 2; round < 30; round++ {
		for i := range series {
			mustAppend(t, e, mkSeries("job", "api", "shard", strconv.Itoa(i)), int64(round*1000+i), float64(i))
		}

		require.NoError(t, e.Flush(ctx))
		require.NoError(t, e.Merge(ctx, 0))
	}

	stop.Store(true)
	wg.Wait()

	if perr := readErr.Load(); perr != nil {
		t.Fatalf("concurrent reader failed during recycle: %v", *perr)
	}
}

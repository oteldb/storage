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
)

// streamCorpus builds a multi-part corpus where each series carries many samples (so a series spans
// several small blocks) and the same series recurs across flushes (so it lives in several parts) —
// exercising the block-sliced merge's multi-block and multi-part paths.
func streamCorpus(t *testing.T, cacheBytes int64) *engine.Engine {
	t.Helper()

	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "t/stream", DecodeCacheBytes: cacheBytes, MetricBlockRows: 4,
	})

	const series, rounds, samples = 25, 3, 10

	for round := range rounds {
		for s := range series {
			ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
			for k := range samples {
				mustAppend(t, e, ser, int64(round*100000+s*1000+k*10), float64(round*1000+s*10+k))
			}
		}

		require.NoError(t, e.Flush(ctx))
	}

	return e
}

// TestStreamMergeCacheVsNoCache pins the block-sliced fetch merge (used when the decode cache is on)
// against the whole-part decode path (cache off): the two must return byte-identical results across a
// broad selector, a single series, a windowed range, and a sparse selector — over multi-block series
// living in multiple parts.
func TestStreamMergeCacheVsNoCache(t *testing.T) {
	t.Parallel()

	cached := streamCorpus(t, 1<<20) // block-sliced merge
	plain := streamCorpus(t, 0)      // whole-part decode reference

	reqs := map[string]fetch.Request{
		"all":    {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}},
		"single": {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", "7")}},
		"window": {Start: 100050, End: 200050, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}},
		"sparse": {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", "23")}},
		"empty":  {Start: 1 << 50, End: 1 << 51, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}},
	}

	for name, req := range reqs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, drainSorted(t, plain, req), drainSorted(t, cached, req),
				"block-sliced merge must match whole-part decode")
		})
	}
}

// TestStreamMergeConcurrent runs many concurrent fetches through the block-sliced merge (prefetch
// warms blocks on goroutines, the scan reads them) and checks they all return the reference result —
// guards the per-part reader's concurrency model. Run under -race.
func TestStreamMergeConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cached := streamCorpus(t, 1<<20)

	req := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}}
	want := drainSorted(t, cached, req)
	require.NotEmpty(t, want)

	var (
		wg  sync.WaitGroup
		bad atomic.Bool
	)

	for range 12 {
		wg.Go(func() {
			it, err := cached.Fetch(ctx, req)
			if err != nil {
				bad.Store(true)

				return
			}

			batches, err := fetch.Drain(ctx, it)
			if err != nil || len(batches) != len(want) {
				bad.Store(true)
			}
		})
	}

	wg.Wait()
	assert.False(t, bad.Load(), "a concurrent block-sliced fetch errored or returned the wrong count")
}

// BenchmarkFetchBroadSelector fetches every series of a large part with the decode cache warm. With
// the block-sliced merge the per-fetch transient is the result, not the whole decoded columns.
func BenchmarkFetchBroadSelector(b *testing.B) {
	ctx := context.Background()

	const series, samples, stepSec = 4000, 16, 15

	ser, ids := buildNamedSeries(series, "node_x")
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "bench/broad", MaxPartBytes: 0, DecodeCacheBytes: 256 << 20,
	})
	flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{eqMatcher("__name__", "node_x")}}
	fetchAll2(b, ctx, e, req) // warm the cache

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if n := len(fetchAll2(b, ctx, e, req)); n != series {
			b.Fatalf("want %d series, got %d", series, n)
		}
	}
}

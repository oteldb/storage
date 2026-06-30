package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestBlockCacheReuseAcrossSeries pins the block cache's cross-fetch behavior: re-fetching a series
// decodes nothing (its blocks are cached), while fetching a *new* series decodes only that series'
// blocks (the others stay cached). It exercises the (part, column, block) key — a sparse selector
// caches only the blocks it touches, and overlapping queries reuse them.
func TestBlockCacheReuseAcrossSeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "t/bc", DecodeCacheBytes: 1 << 20, MetricBlockRows: 8,
	})

	const series, samples = 80, 6
	for s := range series {
		ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
		for k := range samples {
			mustAppend(t, e, ser, int64(1000+s*100+k), float64(s*10+k))
		}
	}

	require.NoError(t, e.Flush(ctx))

	one := func(host string) fetch.Request {
		return fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", host)}}
	}

	// First fetch of host 5: decodes and caches its blocks.
	require.Len(t, fetchAll(t, e, one("5")), 1)
	st1, ok := e.DecodeCacheStats()
	require.True(t, ok)
	require.Positive(t, st1.Items)

	// Re-fetch host 5: every block is a cache hit, nothing re-decoded.
	require.Len(t, fetchAll(t, e, one("5")), 1)
	st2, _ := e.DecodeCacheStats()
	assert.Equal(t, st1.Misses, st2.Misses, "re-fetching the same series re-decodes no block")
	assert.Greater(t, st2.Hits, st1.Hits, "the repeat fetch hits the cache")

	// Fetch a different series: its blocks miss and are decoded, host 5's stay cached.
	require.Len(t, fetchAll(t, e, one("6")), 1)
	st3, _ := e.DecodeCacheStats()
	assert.Greater(t, st3.Misses, st2.Misses, "a new series decodes its own blocks")
	assert.Greater(t, st3.Items, st1.Items, "the cache grows by the new series' blocks")

	// A sparse fetch caches far fewer blocks than touching every series would: confirm the resident
	// block count is well below the whole part's block count after only two series.
	full := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}}
	require.Len(t, fetchAll(t, e, full), series)
	stFull, _ := e.DecodeCacheStats()
	assert.Greater(t, stFull.Items, st3.Items, "fetching every series caches the rest of the blocks")
}

// BenchmarkBlockCacheFetch measures the cached-path series-skip win: a sparse selector repeatedly
// fetched. With the block cache the repeat fetch is served from cached blocks (no decode); without a
// cache every fetch re-decodes the touched blocks.
func BenchmarkBlockCacheFetch(b *testing.B) {
	ctx := context.Background()

	const series, samples, stepSec = 4000, 16, 15

	for _, bc := range []struct {
		name  string
		bytes int64
	}{
		{"cached", 256 << 20},
		{"nocache", 0},
	} {
		b.Run(bc.name, func(b *testing.B) {
			ser, ids := buildNamedSeries(series, "node_disk_read_bytes_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/bc", MaxPartBytes: 0, DecodeCacheBytes: bc.bytes,
			})
			flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

			req := fetch.Request{
				Start: 0, End: 1 << 62,
				Matchers: []fetch.Matcher{eqMatcher("instance", "host-2001")},
			}

			if n := len(fetchAll2(b, ctx, e, req)); n != 1 {
				b.Fatalf("want 1, got %d", n)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if n := len(fetchAll2(b, ctx, e, req)); n != 1 {
					b.Fatalf("want 1, got %d", n)
				}
			}
		})
	}
}

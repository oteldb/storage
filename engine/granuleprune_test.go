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

// granulePruneEngine builds a one-part engine holding nSeries series of samples spaced stepMs apart
// from ts 0, blocked at blockRows with the block cache on (granule pruning only runs on the
// block-sliced read path).
func granulePruneEngine(t *testing.T, nSeries, samples, blockRows int, stepMs int64) *engine.Engine {
	t.Helper()

	e := engine.New(engine.Config{
		Backend:          backend.Memory(),
		Prefix:           "t/granule",
		MetricBlockRows:  blockRows,
		DecodeCacheBytes: 1 << 24,
	})

	for s := range nSeries {
		ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
		for k := range samples {
			mustAppend(t, e, ser, int64(k)*stepMs, float64(s*1000+k))
		}
	}

	require.NoError(t, e.Flush(context.Background()))
	require.Positive(t, e.PartCount())

	return e
}

// TestGranulePruneEquivalence checks that skipping blocks by their granule time bounds cannot change
// a result: for a part whose series each span far more time than the query, every window must return
// exactly what the same store returns when no block can be pruned (a single part-sized block).
func TestGranulePruneEquivalence(t *testing.T) {
	t.Parallel()

	const (
		nSeries = 8
		samples = 64
		stepMs  = 1000
	)

	fine := granulePruneEngine(t, nSeries, samples, 8, stepMs)
	whole := granulePruneEngine(t, nSeries, samples, nSeries*samples, stepMs)

	all := []fetch.Matcher{eqMatcher("__name__", "m")}
	one := []fetch.Matcher{eqMatcher("host", "3")}

	reqs := map[string]fetch.Request{
		"full range":             {Start: 0, End: 1 << 40, Matchers: all},
		"one sample":             {Start: 5000, End: 5000, Matchers: all},
		"inside a block":         {Start: 9000, End: 12000, Matchers: all},
		"spanning blocks":        {Start: 7000, End: 41000, Matchers: all},
		"leading edge":           {Start: 0, End: 3000, Matchers: all},
		"trailing edge":          {Start: 60000, End: 1 << 40, Matchers: all},
		"before every sample":    {Start: -5000, End: -1, Matchers: all},
		"after every sample":     {Start: 1 << 20, End: 1 << 40, Matchers: all},
		"single series windowed": {Start: 20000, End: 24000, Matchers: one},
	}

	for name, req := range reqs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, drainSorted(t, whole, req), drainSorted(t, fine, req),
				"granule-pruned result must match the unprunable reference")
		})
	}
}

// TestGranulePruneSkipsBlocks pins the point of the change: a narrow window over a part whose series
// span the whole store must decode only the blocks that can hold an in-window sample. Cache misses
// are the blocks actually read, so a cold engine's miss count is the work the fetch did. Before
// granule pruning a series' whole row range was decoded and then discarded row by row, which is
// what made a one-hour query cost the same as a three-day one.
func TestGranulePruneSkipsBlocks(t *testing.T) {
	t.Parallel()

	const (
		nSeries   = 4
		samples   = 128
		blockRows = 8
		stepMs    = 1000
	)

	misses := func(req fetch.Request) int64 {
		// A cold engine per measurement: every block the fetch touches is a miss.
		e := granulePruneEngine(t, nSeries, samples, blockRows, stepMs)

		fetchAll(t, e, req)

		st, ok := e.DecodeCacheStats()
		require.True(t, ok)

		return st.Misses
	}

	all := []fetch.Matcher{eqMatcher("__name__", "m")}

	full := misses(fetch.Request{Start: 0, End: 1 << 40, Matchers: all})
	narrow := misses(fetch.Request{Start: 4000, End: 6000, Matchers: all})

	require.Positive(t, full)
	assert.Less(t, narrow, full, "a narrow window must decode fewer blocks than the full range")

	// The window covers 3 of 128 sample positions per series, so it can need at most one block per
	// series per column plus the boundary block — far below the full scan either way.
	assert.Less(t, narrow, full/4, "a window over ~2% of the range must not cost a quarter of a full scan")
}

// BenchmarkFetchGranulePrune measures the issue-308 shape: a part whose series each span the whole
// store, queried over a small slice of that span. "full" scans everything (nothing to prune, the
// ceiling); the narrower windows should cost in proportion to the window rather than to the part.
func BenchmarkFetchGranulePrune(b *testing.B) {
	ctx := context.Background()

	const series, samples, stepSec, blockRows = 200, 2048, 15, 256

	span := int64(samples) * stepSec // the full time range every series covers

	for _, bc := range []struct {
		name string
		frac int64 // window is span/frac
	}{
		{"full", 1},
		{"half", 2},
		{"tenth", 10},
		{"hundredth", 100},
	} {
		b.Run(bc.name, func(b *testing.B) {
			ser, ids := buildNamedSeries(series, "node_cpu_seconds_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/granule",
				MaxPartBytes: 0, MetricBlockRows: blockRows, DecodeCacheBytes: 1 << 28,
			})
			flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

			req := fetch.Request{
				Start:    0,
				End:      span / bc.frac,
				Matchers: []fetch.Matcher{eqMatcher("__name__", "node_cpu_seconds_total")},
			}

			if n := len(fetchAll2(b, ctx, e, req)); n != series {
				b.Fatalf("want %d matched series, got %d", series, n)
			}

			b.ReportAllocs()

			for b.Loop() {
				fetchAll2(b, ctx, e, req)
			}
		})
	}
}

// TestGranulePruneEmptyWindow checks the degenerate case: a window that no granule overlaps must
// decode nothing at all and return nothing.
func TestGranulePruneEmptyWindow(t *testing.T) {
	t.Parallel()

	e := granulePruneEngine(t, 4, 64, 8, 1000)

	got := fetchAll(t, e, fetch.Request{
		Start: 1 << 30, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")},
	})
	assert.Empty(t, got)

	st, ok := e.DecodeCacheStats()
	require.True(t, ok)
	assert.Zero(t, st.Misses, "a window past every sample must decode no block")
}

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

// TestFetchSeriesSkipEquivalence checks the series-skip decode path: an engine that blocks parts at a
// tiny block size (so a fetch decodes only a few of many blocks) must return byte-identical results
// to one that blocks at a part-sized block (a single block, the whole-decode reference) — across a
// sparse single-series selector, a multi-series selector, and a windowed selector. Both engines use
// no decode cache, so both exercise the series-skip path; only the block granularity differs.
func TestFetchSeriesSkipEquivalence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const series, samples = 40, 5

	build := func(blockRows int) *engine.Engine {
		e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/skip", MetricBlockRows: blockRows})
		for s := range series {
			ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
			for k := range samples {
				mustAppend(t, e, ser, int64(100+s*1000+k*10), float64(s*100+k))
			}
		}

		require.NoError(t, e.Flush(ctx))

		return e
	}

	fine := build(4)         // series×samples = 200 rows ⇒ 50 blocks of 4
	coarse := build(1 << 20) // a single block ⇒ whole-column decode reference
	require.Positive(t, fine.PartCount())

	reqs := map[string]fetch.Request{
		"single": {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", "7")}},
		"all":    {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}},
		"window": {Start: 7110, End: 7120, Matchers: []fetch.Matcher{eqMatcher("host", "7")}},
		"sparse": {Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", "23")}},
	}

	for name, req := range reqs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			gotFine := drainSorted(t, fine, req)
			gotCoarse := drainSorted(t, coarse, req)
			assert.Equal(t, gotCoarse, gotFine, "series-skip result must match whole-decode")
		})
	}

	// Spot-check the actual values for the single-series selector (series 7: 5 samples from ts 7100).
	got := fetchAll(t, fine, reqs["single"])
	require.Len(t, got, 1)
	assert.Equal(t, []int64{7100, 7110, 7120, 7130, 7140}, got[0].Timestamps)
	assert.Equal(t, []float64{700, 701, 702, 703, 704}, got[0].Values)
}

// drainSorted returns each matched series' (id, timestamps, values) for a request, as a comparable
// map keyed by the series id string.
func drainSorted(t *testing.T, e *engine.Engine, req fetch.Request) map[string][]float64 {
	t.Helper()

	out := make(map[string][]float64)

	for _, b := range fetchAll(t, e, req) {
		row := make([]float64, 0, len(b.Timestamps)*2)
		for i := range b.Timestamps {
			row = append(row, float64(b.Timestamps[i]), b.Values[i])
		}

		out[b.ID.String()] = row
	}

	return out
}

// BenchmarkFetchSeriesSkip measures the series-skip win: a sparse selector (one of many series) over
// a multi-block part should decode only the blocks spanning that series. The sub-benchmarks vary the
// block size — "whole" is a single part-sized block (no skip, the baseline); finer sizes skip more.
func BenchmarkFetchSeriesSkip(b *testing.B) {
	ctx := context.Background()

	const series, samples, stepSec = 4000, 16, 15

	for _, bc := range []struct {
		name      string
		blockRows int
	}{
		{"whole", series * samples}, // one block ⇒ whole-column decode
		{"block1024", 1024},
		{"block256", 256},
	} {
		b.Run(bc.name, func(b *testing.B) {
			ser, ids := buildNamedSeries(series, "node_disk_read_bytes_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/skip", MaxPartBytes: 0, MetricBlockRows: bc.blockRows,
			})
			flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

			// A single instance ⇒ one matched series scattered in the part by SeriesID hash.
			req := fetch.Request{
				Start:    0,
				End:      1 << 62,
				Matchers: []fetch.Matcher{eqMatcher("instance", "host-2001")},
			}

			if n := len(fetchAll2(b, ctx, e, req)); n != 1 {
				b.Fatalf("want 1 matched series, got %d", n)
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

// fetchAll2 is the benchmark-side fetch+drain (fetchAll takes *testing.T).
func fetchAll2(b *testing.B, ctx context.Context, e *engine.Engine, req fetch.Request) []*fetch.Batch {
	b.Helper()

	it, err := e.Fetch(ctx, req)
	if err != nil {
		b.Fatal(err)
	}

	got, err := fetch.Drain(ctx, it)
	if err != nil {
		b.Fatal(err)
	}

	return got
}

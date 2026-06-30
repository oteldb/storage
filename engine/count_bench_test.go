package engine_test

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// BenchmarkCountPushdown reproduces the full_scan_count finding: count({...}) over a large
// matched set. It contrasts the current path (Fetch → materialize every matched series'
// samples + labels) with Engine.Count (resolve matchers + per-series in-window existence,
// no materialization).
//
// full_scan_count is `count({__name__=~"node_.+"})` — ~1.3k metrics × N hosts. Here we build
// one flushed part with `series` series sharing a broad match, then count them. The Fetch
// baseline pays per-series sample gathering + the result batches; Count pays only the
// decode-once-per-part + a binary search per series. The delta is the pushdown's win.
func BenchmarkCountPushdown(b *testing.B) {
	for _, tc := range []struct {
		name   string
		series int
	}{
		{"1k", 1000},
		{"10k", 10000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()

			const samples, stepSec, parts = 15, 15, 1

			ser, ids := buildNamedSeries(tc.series, "node_disk_read_bytes_total")
			e := engine.New(engine.Config{
				Backend: backend.Memory(), Prefix: "bench/metrics", MaxPartBytes: 0,
			})
			flushParts(b, ctx, e, ser, ids, samples, stepSec, parts)

			req := fetch.Request{
				Start:    0,
				End:      int64(parts*samples*stepSec) * 1e9,
				Matchers: []fetch.Matcher{eqMatcher("__name__", "node_disk_read_bytes_total")},
			}

			// Sanity: Fetch and Count must agree on cardinality before we time them.
			want := countViaFetch(b, ctx, e, req)
			got, err := e.Count(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if got != want {
				b.Fatalf("count mismatch: Count=%d Fetch=%d", got, want)
			}

			b.Run("Fetch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if n := countViaFetch(b, ctx, e, req); n != want {
						b.Fatalf("count=%d want %d", n, want)
					}
				}
			})

			b.Run("Count", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if n, err := e.Count(ctx, req); err != nil {
						b.Fatal(err)
					} else if n != want {
						b.Fatalf("count=%d want %d", n, want)
					}
				}
			})
		})
	}
}

// BenchmarkCountColumnPrune measures the column-need pruning win on the count decode path. A count
// whose window only partially overlaps the part (so the fully-covered index shortcut does NOT apply)
// must decode the part to test in-window existence — but it reads only timestamps, so it skips the
// Gorilla-XOR value column. The window starts mid-part to force the partial-overlap decode of the
// whole part.
func BenchmarkCountColumnPrune(b *testing.B) {
	ctx := context.Background()

	const series, samples, stepSec, parts = 5000, 30, 15, 1

	ser, ids := buildNamedSeries(series, "node_disk_read_bytes_total")
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "bench/prune", MaxPartBytes: 0})
	flushParts(b, ctx, e, ser, ids, samples, stepSec, parts)

	// Start mid-part (> the part's minTime of 0) so the part is partially covered → decode path, not
	// the zero-decode fully-covered shortcut. End is open so the part's maxTime is inside the window.
	req := fetch.Request{
		Start:    int64(samples*stepSec) / 2,
		End:      1 << 62,
		Matchers: []fetch.Matcher{eqMatcher("__name__", "node_disk_read_bytes_total")},
	}

	want, err := e.Count(ctx, req)
	if err != nil {
		b.Fatal(err)
	}

	if want == 0 {
		b.Fatal("window covers no samples; the decode path would be trivial — adjust bounds")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if n, err := e.Count(ctx, req); err != nil {
			b.Fatal(err)
		} else if n != want {
			b.Fatalf("count=%d want %d", n, want)
		}
	}
}

// countViaFetch is the pre-pushdown path: drain every matched series' batch and count them.
func countViaFetch(b *testing.B, ctx context.Context, e *engine.Engine, req fetch.Request) int {
	b.Helper()

	it, err := e.Fetch(ctx, req)
	if err != nil {
		b.Fatal(err)
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		b.Fatal(err)
	}

	return len(batches)
}

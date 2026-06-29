package engine_test

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// These benchmarks reproduce the disk_io pprof finding: the per-fetch decode-buffer
// allocation churn through Engine.decPool (a sync.Pool).
//
// disk_io is `sum by(instance,device)(rate(node_disk_read_bytes_total{...}[5m]))` — its
// hot path is Engine.Fetch → decodeOf → part.decodeInto → chunk.DecodeFloats /
// DecodeTimestamps. Crucially decodeInto decodes the WHOLE part column (O(part rows))
// even when a fetch matches a single series, so the decode buffers scale with part size,
// not matched-series count. The decodedPart ts/vals buffers recycle through Engine.decPool.
//
// The pprof showed ~38 GB/35 s of chunk.resize allocation: sync.Pool is cleared on every
// GC, so under allocation-driven GC pressure (concurrent fetches + the Compressor/slices
// allocators) the decode buffers lose their capacity and chunk.resize reallocates from
// zero each fetch. These benchmarks isolate that cost:
//
//   - BenchmarkDecodeBufferRecycle: a large part, matching ONE series, so nearly every
//     measured allocation is the decode buffer (ts/vals column), not the result path.
//     "steady" lets the pool recycle; "underGC" forces a collection per fetch to reproduce
//     the pool-cleared-by-GC condition. A correct GC-stable recycler makes both converge
//     to ~0 decode allocs.
//   - BenchmarkDecodeBufferRecycleParallel: RunParallel reproduces the contention +
//     sustained GC of production, where multiple in-flight fetches and concurrent
//     allocations keep the pool drained.
//
// Compare allocs/op and B/op before/after swapping Engine.decPool for a zero-alloc
// recycler. The win shows up as the "underGC"/parallel numbers collapsing toward steady.
func BenchmarkDecodeBufferRecycle(b *testing.B) {
	const (
		partSeries = 300 // full disk_io fan-out lives in ONE part
		samples    = 200 // samples/series → part rows = partSeries×samples (decode-buffer sized)
		stepSec    = 15
		metric     = "node_disk_read_bytes_total"
	)

	for _, tc := range []struct {
		name    string
		forceGC int // GCs forced per iteration (sync.Pool victim cache survives 1 GC; ≥2 drains it)
	}{
		{"steady", 0},
		{"underGC", 2},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()

			ser, ids := buildNamedSeries(partSeries, metric)
			e := engine.New(engine.Config{
				Backend:      backend.Memory(),
				Prefix:       "bench/metrics",
				MaxPartBytes: 0,
			})
			flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

			// Match a SINGLE series: the part is still decoded in full (decodeInto reads the
			// whole ts/value column), so the measured allocation is the decode buffer, not the
			// per-matched-series result batches.
			req := fetch.Request{
				Start:    0,
				End:      int64(samples*stepSec) * 1e9,
				Matchers: []fetch.Matcher{eqMatcher("instance", "host-0")},
			}
			warmFetch(b, ctx, e, req, 1)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				it, err := e.Fetch(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := fetch.Drain(ctx, it); err != nil {
					b.Fatal(err)
				}
				for range tc.forceGC {
					runtime.GC()
				}
			}
		})
	}
}

// BenchmarkDecodeBufferRecycleParallel drives concurrent fetches (production shape). In
// production the sync.Pool is kept drained not by these fetches alone but by the
// concurrent Compressor.Decompress / slices.Clone allocators (7+ GB each in the disk_io
// pprof) driving continuous GC; we reproduce that pressure with a low GOGC + a forced
// collection every few fetches, so the pool's per-P and victim caches stay drained.
// A zero-alloc recycler holds its buffers across this and collapses B/op toward steady.
func BenchmarkDecodeBufferRecycleParallel(b *testing.B) {
	const (
		partSeries = 300
		samples    = 200
		stepSec    = 15
		metric     = "node_disk_read_bytes_total"
	)

	// Aggressive GC stands in for the concurrent allocators that drain the pool in prod.
	prev := debug.SetGCPercent(20)
	defer debug.SetGCPercent(prev)

	ctx := context.Background()

	ser, ids := buildNamedSeries(partSeries, metric)
	e := engine.New(engine.Config{
		Backend:      backend.Memory(),
		Prefix:       "bench/metrics",
		MaxPartBytes: 0,
	})
	flushParts(b, ctx, e, ser, ids, samples, stepSec, 1)

	req := fetch.Request{
		Start:    0,
		End:      int64(samples*stepSec) * 1e9,
		Matchers: []fetch.Matcher{eqMatcher("instance", "host-0")},
	}
	warmFetch(b, ctx, e, req, 1)

	b.ReportAllocs()
	b.ResetTimer()

	var i atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			it, err := e.Fetch(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := fetch.Drain(ctx, it); err != nil {
				b.Fatal(err)
			}
			// Force a collection roughly every other fetch to model the production GC rate
			// that keeps sync.Pool drained (the pprof showed >50% of CPU in GC).
			if i.Add(1)%2 == 0 {
				runtime.GC()
			}
		}
	})
}

// buildNamedSeries makes n distinct series sharing __name__=name, distinguished by an
// instance label so they hash distinctly (mirrors the host/device fan-out).
func buildNamedSeries(n int, name string) ([]signal.Series, []signal.SeriesID) {
	ser := make([]signal.Series, n)
	ids := make([]signal.SeriesID, n)

	for i := range n {
		ser[i] = mkSeries("__name__", name, "instance", "host-"+itoa(i), "device", "sda")
		ids[i] = ser[i].Hash()
	}

	return ser, ids
}

// flushParts writes `samples` consecutive samples per series into one flushed part.
func flushParts(b *testing.B, ctx context.Context, e *engine.Engine, ser []signal.Series, ids []signal.SeriesID, samples, stepSec, parts int) {
	b.Helper()

	n := len(ids) * samples
	batchIDs := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	vals := make([]float64, n)

	for p := range parts {
		k := 0
		for i := range ids {
			for s := range samples {
				batchIDs[k] = ids[i]
				ts[k] = int64(p*samples+s)*int64(stepSec) + int64(i)
				vals[k] = float64(i)
				k++
			}
		}

		resolve := func(i int) signal.Series { return ser[i/samples] }
		if _, err := e.AppendBatch(batchIDs, ts, vals, nil, resolve, engine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}

		if err := e.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func warmFetch(b *testing.B, ctx context.Context, e *engine.Engine, req fetch.Request, want int) {
	b.Helper()

	it, err := e.Fetch(ctx, req)
	if err != nil {
		b.Fatal(err)
	}

	got, err := fetch.Drain(ctx, it)
	if err != nil {
		b.Fatal(err)
	}

	if len(got) != want {
		b.Fatalf("warmup: expected %d batches, got %d", want, len(got))
	}
}

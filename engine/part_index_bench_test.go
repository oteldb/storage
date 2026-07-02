package engine

import (
	"context"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// makeSortedSeriesColumn builds the sorted SeriesID column a part stores on disk (each distinct id
// repeated for its sample run), the exact shape openPart reads to build its index. n distinct series,
// each with samplesPerSeries rows, ordered by SeriesID so each id's run is contiguous.
func makeSortedSeriesColumn(n, samplesPerSeries int) []chunk.U128 {
	ids := make([]signal.SeriesID, n)
	for i := range n {
		// Distinct, well-spread ids; Lo bit 1 keeps them nonzero.
		ids[i] = signal.SeriesID{Hi: uint64(i) >> 1, Lo: uint64(i)*0x9e3779b97f4a7c15 | 1}
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	out := make([]chunk.U128, 0, n*samplesPerSeries)
	for _, id := range ids {
		for range samplesPerSeries {
			out = append(out, chunk.U128{Hi: id.Hi, Lo: id.Lo})
		}
	}

	return out
}

// BenchmarkPartIndexBuild measures the cost openPart pays once per part: scanning the sorted series
// column and building the resident SeriesID→row-range index. This is the per-part resident-heap line
// item that dominates live heap under continuous ingestion (issue #25 root cause B).
func BenchmarkPartIndexBuild(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		const samplesPerSeries = 4

		col := makeSortedSeriesColumn(n, samplesPerSeries)

		b.Run(strconv.Itoa(n/1000)+"k", func(b *testing.B) {
			b.ReportAllocs()
			// Size by the logical index payload: one 16-byte id per series.
			b.SetBytes(int64(n) * 16)

			for range b.N {
				idx := buildPartIndex(col)
				if len(idx.ids) != n {
					b.Fatalf("got %d series, want %d", len(idx.ids), n)
				}
			}
		})
	}
}

// BenchmarkPartIndexLookup measures the per-query cost of locating a series' row range in a part's
// index — paid once per (part × series a query touches) — for both index forms: the resident
// slices and the paged sidecar view (binary search over raw fixed-width entries). The paged form
// is the hot-path cost the pageable index trades for its zero pinned heap.
func BenchmarkPartIndexLookup(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		const samplesPerSeries = 4

		col := makeSortedSeriesColumn(n, samplesPerSeries)
		idx := buildPartIndex(col)

		be := backend.Memory()
		if err := be.Write(context.Background(), sidxKey("p"), encodeSeriesIndex(col)); err != nil {
			b.Fatal(err)
		}

		paged, ok := openPagedIndex(context.Background(), be, "p", len(col))
		if !ok {
			b.Fatal("paged index did not open")
		}

		pagedIdx := partIndex{paged: paged}

		// A realistic mix: ~half the queries hit a resident series, half miss (a range query over a
		// time window touches some parts that don't hold a given series).
		probes := make([]signal.SeriesID, 0, n*2)
		for i := range n {
			id := idx.ids[i]
			probes = append(probes, id) // a hit
			probes = append(probes, signal.SeriesID{
				Hi: id.Hi,
				Lo: id.Lo ^ 0xdeadbeef,
			}) // a nearby miss
		}

		rng := rand.New(rand.NewChaCha8([32]byte{}))
		for i := len(probes) - 1; i > 0; i-- {
			j := rng.IntN(i + 1)
			probes[i], probes[j] = probes[j], probes[i]
		}

		for name, ix := range map[string]partIndex{"resident": idx, "paged": pagedIdx} {
			b.Run(strconv.Itoa(n/1000)+"k/"+name, func(b *testing.B) {
				b.ReportAllocs()

				var sink int

				ctx := context.Background()

				for i := range b.N {
					rg, ok, _ := ix.lookup(ctx, probes[i%len(probes)])
					if ok {
						sink += rg.end - rg.start
					}
				}

				_ = sink
			})
		}
	}
}

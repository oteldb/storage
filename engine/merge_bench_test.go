package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkMergeResidentMemory is a regression benchmark for the background merge over several
// fully-overlapping same-tier parts (the steady-workload shape). It confirms the streaming k-way
// merge (issue #25, item 1) adds no allocation overhead vs the prior full-decode path: the per-part
// scratch buffers recycle across the series of a merge, so cumulative allocation is byte-identical.
//
// The streaming merge's win is peak *resident* memory, which alloc-byte benchmarks do not capture:
// each source column is held as one read-ahead window and decoded one series range at a time, so
// neither a source's decoded nor its encoded column is resident. TestMergeResidentFlatInPartSize
// measures it; the file cases here price the ranged reads that buy it.
//
// Throughput is the logical samples merged × [engine.SampleBytes], neither the parts' on-disk nor
// their decoded bytes, so its MB/s does not compare with the record engine's merge benchmarks.
func BenchmarkMergeResidentMemory(b *testing.B) {
	memory, onDisk := backendtest.Memory(), backendtest.Dir("file", file.New)

	for _, cfg := range []struct {
		name    string
		series  int
		samples int
		parts   int
		backend backendtest.Case
	}{
		{"200s60x4p", 200, 60, 4, memory},
		{"500s400x4p", 500, 400, 4, memory},
		{"file-500s400x4p", 500, 400, 4, onDisk},
		{"file-500s2000x4p", 500, 2000, 4, onDisk},
	} {
		b.Run(cfg.name, func(b *testing.B) {
			ctx := context.Background()

			series := make([]signal.Series, cfg.series)
			ids := make([]signal.SeriesID, cfg.series)

			for i := range cfg.series {
				series[i] = mkSeries("__name__", "cpu", "host", "h"+strconv.Itoa(i))
				ids[i] = series[i].Hash()
			}

			b.ReportAllocs()
			b.SetBytes(int64(cfg.series*cfg.samples*cfg.parts) * engine.SampleBytes)
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				// Unlimited part size (MaxPartBytes 0) so each flush is one part; merge compacts the
				// cfg.parts flushes into one.
				e := engine.New(engine.Config{
					Backend: cfg.backend.Open(b), Prefix: "default/metrics", MaxPartBytes: 0,
				})

				flushCorpus(b, ctx, e, series, ids, cfg.samples, cfg.parts,
					func(p, i, s int) int64 { return int64(p*cfg.samples+s)*15 + int64(i) },
					func(p, i, _ int) float64 { return float64(p*cfg.series + i) })

				b.StartTimer()

				if err := e.Merge(ctx, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

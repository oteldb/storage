package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkMergeResidentMemory measures the background merge's decoded-input working set — the cost
// the streaming k-way merge (issue #25, item 1) targets. It builds several same-tier parts (each a
// flush of the same series set with several samples, so they fully overlap — the steady-workload
// shape where the old path held every source part's whole decoded column resident for the whole
// merge) and merges them once.
//
// The streaming merge decodes one series range at a time per part into reusable scratch, so the
// resident decoded input is O(parts × one-series-range) instead of O(parts × whole-column); the
// alloc/byte delta grows with samples-per-series (the column size the old path materializes).
func BenchmarkMergeResidentMemory(b *testing.B) {
	for _, cfg := range []struct {
		name    string
		series  int
		samples int
		parts   int
	}{
		{"200s60x4p", 200, 60, 4},
		{"2000s60x4p", 2000, 60, 4},
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
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				// Unlimited part size (MaxPartBytes 0) so each flush is one part; merge compacts the
				// cfg.parts flushes into one.
				e := engine.New(engine.Config{
					Backend: backend.Memory(), Prefix: "default/metrics", MaxPartBytes: 0,
				})

				for p := range cfg.parts {
					ts := make([]int64, cfg.series*cfg.samples)
					vals := make([]float64, cfg.series*cfg.samples)

					k := 0
					for i := range cfg.series {
						for s := range cfg.samples {
							ts[k] = int64(p*cfg.samples+s)*15 + int64(i)
							vals[k] = float64(p*cfg.series + i)
							k++
						}
					}

					if _, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return series[i%cfg.series] }, engine.AppendLimits{}); err != nil {
						b.Fatal(err)
					}

					if err := e.Flush(ctx); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()

				if err := e.Merge(ctx, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

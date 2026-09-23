package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
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
// the old path held every source part's whole decoded column resident simultaneously during the
// merge, while the streaming path holds one series range per part. That win scales with part size
// (at the production 64 MiB part / 8-way merge the old path pinned ≈256 MiB; the streaming path
// holds ≈ KB) and is verified structurally — the cursors decode one range at a time into reused
// scratch, never a whole column.
func BenchmarkMergeResidentMemory(b *testing.B) {
	for _, cfg := range []struct {
		name    string
		series  int
		samples int
		parts   int
		file    bool
	}{
		{"200s60x4p", 200, 60, 4, false},
		{"500s400x4p", 500, 400, 4, false},
		{"file-500s400x4p", 500, 400, 4, true},
		{"file-500s2000x4p", 500, 2000, 4, true},
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
			b.SetBytes(int64(cfg.series * cfg.samples * cfg.parts * 16))
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				// Unlimited part size (MaxPartBytes 0) so each flush is one part; merge compacts the
				// cfg.parts flushes into one.
				be := backend.Memory()
				if cfg.file {
					var err error
					if be, err = file.New(b.TempDir()); err != nil {
						b.Fatal(err)
					}
				}

				e := engine.New(engine.Config{
					Backend: be, Prefix: "default/metrics", MaxPartBytes: 0,
				})

				for p := range cfg.parts {
					// cfg.samples consecutive samples per series in this flush: ids repeats each
					// series id cfg.samples times (AppendBatch writes one sample per ids[i]).
					n := cfg.series * cfg.samples
					batchIDs := make([]signal.SeriesID, n)
					ts := make([]int64, n)
					vals := make([]float64, n)

					k := 0
					for i := range cfg.series {
						for s := range cfg.samples {
							batchIDs[k] = ids[i]
							ts[k] = int64(p*cfg.samples+s)*15 + int64(i)
							vals[k] = float64(p*cfg.series + i)
							k++
						}
					}

					if _, err := e.AppendBatch(batchIDs, ts, vals, nil, func(i int) signal.Series { return series[i/cfg.samples] }, engine.AppendLimits{}); err != nil {
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

package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkPartCountGrowth drives the steady-state shape issue #25 root cause A describes: a
// continuous ingestor that flushes a part every interval while a background merge runs each cycle.
// It reports the part count after each flush+merge round, so part-count growth (today unbounded —
// once a part reaches the per-part cap it is sealed and never re-compacted) is visible as a
// benchmark artifact and a before/after comparison for the tiered-merge work.
//
// Run with:
//
//	go test -run=^$ -bench=^BenchmarkPartCountGrowth$ -benchtime=1x ./engine/
func BenchmarkPartCountGrowth(b *testing.B) {
	for _, cfg := range []struct {
		name    string
		maxRows int
	}{
		{"cap5", 5},    // tiny cap: every flush seals a part (issue's micro shape)
		{"cap50", 50},  // small cap
		{"default", 0}, // the production default (64 MiB) — no sealing within this bench's volume
	} {
		b.Run(cfg.name, func(b *testing.B) {
			ctx := context.Background()

			// cap5/cap50 translate MaxPartBytes from a row count for the micro case; default leaves
			// MaxPartBytes unset (the engine applies its 64 MiB default).
			ecfg := engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"}
			if cfg.maxRows > 0 {
				ecfg.MaxPartBytes = int64(cfg.maxRows) * 32 // partRowBytes
			}

			e := engine.New(ecfg)

			const seriesPerFlush = 3

			// Reusable series set so successive flushes hit the same ids (the steady-state shape:
			// the same active series re-flushed each interval).
			series := make([]signal.Series, seriesPerFlush)
			ids := make([]signal.SeriesID, seriesPerFlush)

			for i := range series {
				series[i] = mkSeries("__name__", "cpu", "host", "h"+strconv.Itoa(i))
				ids[i] = series[i].Hash()
			}

			tss := make([]int64, seriesPerFlush)
			vals := make([]float64, seriesPerFlush)

			b.ReportAllocs()
			b.ResetTimer()

			var lastPartCount int

			for i := range b.N {
				ts := int64(100 + i)
				for j := range seriesPerFlush {
					tss[j], vals[j] = ts, float64(i*10+j)
				}

				if _, err := e.AppendBatch(ids, tss, vals, nil, func(k int) signal.Series { return series[k] }, engine.AppendLimits{}); err != nil {
					b.Fatal(err)
				}

				if err := e.Flush(ctx); err != nil {
					b.Fatal(err)
				}

				// A background merge runs each cycle, as the maintenance loop would.
				if err := e.Merge(ctx, 0); err != nil {
					b.Fatal(err)
				}

				lastPartCount = e.PartCount()
			}

			b.StopTimer()
			// Report the final part count as a custom metric so growth is legible in bench output.
			b.ReportMetric(float64(lastPartCount), "parts")
		})
	}
}

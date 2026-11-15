package engine

import (
	"fmt"
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
)

func BenchmarkFrozenHeadParts(b *testing.B) {
	for _, count := range []int{4096, 32768} {
		for _, workers := range []int{1, 2} {
			b.Run(fmt.Sprintf("series=%d/workers=%d", count, workers), func(b *testing.B) {
				file, err := backendfile.New(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				pool := memlimit.NewPool(128 << 20)
				cfg := Config{Backend: file, Prefix: "frozen-bench", DiskSeries: true, MetricSeriesChunks: true,
					ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 32 << 20, MaxPartBytes: 1024 * partRowBytes,
					FlushAdmission: pool.Acquire}
				if workers == 2 {
					cfg.TryFlushAdmission = pool.TryAcquire
				}
				e := New(cfg)
				b.Cleanup(func() { _ = e.CloseWAL() })
				in := nodeExporterInputBatch(count, 0)
				if r := in.append(e); r.err != nil || r.result.Accepted != count {
					b.Fatalf("fixture accepted=%d: %v", r.result.Accepted, r.err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(count) * SampleBytes)
				for b.Loop() {
					parts, rows, err := e.streamHead(b.Context(), e.head.samples)
					if err != nil || rows != count {
						b.Fatalf("build rows=%d: %v", rows, err)
					}
					b.StopTimer()
					closePartReaders(parts)
					for _, p := range parts {
						if err := deletePart(b.Context(), file, p.prefix); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(b.N*count)/b.Elapsed().Seconds(), "samples/s")
			})
		}
	}
}

package engine

import (
	"fmt"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs"
)

// BenchmarkFlushMergeOverlap holds a merge grant to isolate flush admission from merge CPU/I/O.
func BenchmarkFlushMergeOverlap(b *testing.B) {
	count := cpuBenchInt(b, "OTELDB_BENCH_OVERLAP_SERIES", 80000, 640000)
	input := nodeExporterInputBatch(count, 0)
	for _, total := range []int64{320 << 20, 384 << 20} {
		b.Run(fmt.Sprintf("buildMiB=%d", total>>20), func(b *testing.B) {
			file, err := backendfile.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			pool := memlimit.NewPool(total)
			releaseMerge, err := pool.Acquire(b.Context(), 192<<20)
			if err != nil {
				b.Fatal(err)
			}
			defer releaseMerge()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = provider.Shutdown(b.Context()) }()
			observability, err := obs.New(obs.Config{MeterProvider: provider})
			if err != nil {
				b.Fatal(err)
			}
			e := New(Config{Backend: file, Prefix: "overlap", Obs: observability,
				DiskSeries: true, MetricSeriesChunks: true, ResidentHeadBytes: 1 << 30,
				FlushMemoryBytes: 64 << 20, FlushAdmission: pool.Acquire, TryFlushAdmission: pool.TryAcquire})
			defer func() { _ = e.CloseWAL() }()
			if r := input.append(e); r.err != nil || r.result.Accepted != count {
				b.Fatalf("seed accepted=%d: %v", r.result.Accepted, r.err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(count) * SampleBytes)
			var outputs int
			for b.Loop() {
				parts, rows, err := e.streamHead(b.Context(), e.head.samples)
				b.StopTimer()
				if err != nil || rows != count {
					b.Fatalf("rows=%d: %v", rows, err)
				}
				outputs += len(parts)
				closePartReaders(parts)
				for _, p := range parts {
					if err := deletePart(b.Context(), file, p.prefix); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "samples/s")
			b.ReportMetric(float64(outputs)/float64(b.N), "parts/op")
			var data metricdata.ResourceMetrics
			if err := reader.Collect(b.Context(), &data); err != nil {
				b.Fatal(err)
			}
			for _, scope := range data.ScopeMetrics {
				for _, m := range scope.Metrics {
					switch m.Name {
					case "storage.flush.build.workers", "storage.flush.build.jobs":
						unit := "builders/op"
						if m.Name == "storage.flush.build.jobs" {
							unit = "jobs/op"
						}
						for _, point := range m.Data.(metricdata.Histogram[int64]).DataPoints {
							b.ReportMetric(float64(point.Sum)/float64(point.Count), unit)
						}
					case "storage.flush.build.fallbacks":
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							b.ReportMetric(float64(point.Value)/float64(b.N), "fallbacks/op")
						}
					}
				}
			}
		})
	}
}

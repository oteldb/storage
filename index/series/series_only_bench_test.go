package series

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkExporterSeriesOnly defaults to all 1658 captured identities across 10000 hosts.
// Input preparation is outside timing; no samples, WAL, engine or flush work is performed.
func BenchmarkExporterSeriesOnly(b *testing.B) {
	hosts := 10000
	if value := os.Getenv("OTELDB_BENCH_SERIES_HOSTS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 10000 {
			b.Fatal("OTELDB_BENCH_SERIES_HOSTS must be in [1,10000]")
		}
		hosts = n
	}
	for _, mode := range []string{"flat", "instance", "dimensions"} {
		for _, phase := range []string{"register", "existing"} {
			b.Run("hosts="+strconv.Itoa(hosts)+"/"+phase+"/"+mode, func(b *testing.B) {
				fixtureCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "fixture", "workload", phase, "policy", mode))
				ingestCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "ingest", "workload", phase, "policy", mode))
				warmCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "warmup", "workload", phase, "policy", mode))
				defer pprof.SetGoroutineLabels(b.Context())
				pprof.SetGoroutineLabels(fixtureCtx)
				f := newSeriesOnlyFixture(b, hosts)
				var last *Index
				var pool *AttributeTemplates
				if phase == "existing" {
					pool = exporterPool(mode, "cold")
					last = NewWithAttributeTemplates(nil, pool)
					for offset := 0; offset < f.count(); offset += seriesOnlyBatchSize {
						pprof.SetGoroutineLabels(fixtureCtx)
						batch := f.prepareSeriesBatch(offset)
						pprof.SetGoroutineLabels(warmCtx)
						registerSeriesOnlyBatch(last, batch)
					}
				}
				snapshotStarted := false
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					if phase == "register" {
						runtime.GC()
						pool = exporterPool(mode, "cold")
						last = NewWithAttributeTemplates(nil, pool)
					}
					if !snapshotStarted {
						writeSeriesOnlyAllocs(b, "before")
						snapshotStarted = true
					}
					for offset := 0; offset < f.count(); offset += seriesOnlyBatchSize {
						pprof.SetGoroutineLabels(fixtureCtx)
						batch := f.prepareSeriesBatch(offset)
						pprof.SetGoroutineLabels(ingestCtx)
						b.StartTimer()
						registerSeriesOnlyBatch(last, batch)
						b.StopTimer()
					}
					pprof.SetGoroutineLabels(fixtureCtx)
					if last.Len() != f.count() {
						b.Fatalf("identities=%d, want %d", last.Len(), f.count())
					}
					b.StartTimer()
				}
				writeSeriesOnlyAllocs(b, "after")
				b.ReportMetric(float64(f.count())*float64(b.N)/b.Elapsed().Seconds(), "series/s")
				b.ReportMetric(float64(f.count()), "series/op")
				b.ReportMetric(float64(last.SizeBytes()+pool.SizeBytes()), "charged-B")
				runtime.KeepAlive(last)
				runtime.KeepAlive(pool)
			})
		}
	}
}

func registerSeriesOnlyBatch(ix *Index, batch []signal.Series) {
	for i := range batch {
		ix.Add(batch[i])
	}
}

func writeSeriesOnlyAllocs(b *testing.B, stage string) {
	b.Helper()
	dir := os.Getenv("OTELDB_BENCH_SERIES_PROFILE_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	name := strings.NewReplacer("/", "-", "=", "-").Replace(b.Name())
	f, err := os.Create(filepath.Join(dir, name+"-allocs-"+stage+".pb.gz"))
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
		_ = f.Close()
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
}

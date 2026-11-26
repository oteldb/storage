package engine

import (
	"context"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/oteldb/storage/index/series"
)

// BenchmarkHeadRegistration isolates registration of prehashed CPU metric series.
func BenchmarkHeadRegistration(b *testing.B) {
	hosts := cpuBenchInt(b, "OTELDB_BENCH_CPU_HOSTS", 10000, 80000)
	input := nodeExporterInputBatch(hosts*len(cpuFixtureModes), 0)
	entries := make([]series.Entry, len(input.series))
	for i := range entries {
		entries[i] = series.Entry{ID: input.ids[i], Series: input.series[i]}
	}

	for _, name := range []string{"descriptor", "postings", "descriptor-and-postings"} {
		b.Run(name, func(b *testing.B) {
			benchmarkHeadRegistration(b, name, entries)
		})
	}
}

func benchmarkHeadRegistration(b *testing.B, phase string, entries []series.Entry) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		h := newHead()
		if phase == "postings" {
			for i := range entries {
				h.series.AddEntry(entries[i])
			}
		}
		runtime.GC()
		pprof.Do(b.Context(), pprof.Labels("head-registration", phase), func(_ context.Context) {
			b.StartTimer()
			switch phase {
			case "descriptor":
				for i := range entries {
					h.series.AddEntry(entries[i])
				}
			case "postings":
				for i := range entries {
					h.indexLabels(entries[i].ID, entries[i].Series)
				}
			case "descriptor-and-postings":
				for i := range entries {
					h.registerIndex(entries[i])
				}
			}
			b.StopTimer()
		})
		if got, want := h.series.Len(), len(entries); got != want {
			b.Fatalf("head contains %d descriptors, want %d", got, want)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(len(entries)), "series/op")
	b.ReportMetric(float64(b.N)*float64(len(entries))/b.Elapsed().Seconds(), "series/s")
}

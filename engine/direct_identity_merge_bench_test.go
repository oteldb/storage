package engine

import (
	"os"
	"testing"
)

// BenchmarkDiskSeriesCPUMerge measures compaction separately from its durable fixture setup.
// Use -benchtime=1x; the CPU fixture options also control this benchmark.
func BenchmarkDiskSeriesCPUMerge(b *testing.B) {
	options := cpuBenchOptions(b)
	// Keep each generation in one part so the measured merge has identical inputs.
	options.flushMemoryBytes = 2 << 30
	b.ReportAllocs()
	seriesCount := options.hosts * len(cpuFixtureModes)
	samples := seriesCount * options.generations * options.points
	b.SetBytes(int64(samples) * 16)
	profiled := false
	for b.Loop() {
		b.StopTimer()
		f := buildCPUFixture(b, options)
		if f.engine.PartCount() != options.generations {
			b.Fatalf("fixture split: got %d parts, want %d", f.engine.PartCount(), options.generations)
		}
		finishProfile := func() {}
		if !profiled {
			if prefix := os.Getenv("OTELDB_BENCH_CPU_PROFILE"); prefix != "" {
				finish := cpuPhaseProfile(b, prefix+".merge")
				finished := false
				finishProfile = func() {
					if !finished {
						finished = true
						finish()
					}
				}
				b.Cleanup(finishProfile)
			}
			profiled = true
		}
		b.StartTimer()
		if err := f.engine.MergeWith(b.Context(), MergeOptions{Force: true}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		finishProfile()
		if got := f.engine.PartCount(); got != 1 {
			b.Fatalf("merge left %d parts, want 1", got)
		}
		for _, host := range []int{0, options.hosts / 2, options.hosts - 1} {
			if err := f.fetchHost(b.Context(), host); err != nil {
				b.Fatal(err)
			}
		}
		if err := f.engine.Close(b.Context()); err != nil {
			b.Fatal(err)
		}
		f.engine = nil
		f.counter = nil
		if err := os.RemoveAll(f.root); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(seriesCount), "series/op")
	b.ReportMetric(float64(samples), "samples/op")
}

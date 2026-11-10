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
	for b.Loop() {
		b.StopTimer()
		f := buildCPUFixture(b, options)
		if f.engine.PartCount() != options.generations {
			b.Fatalf("fixture split: got %d parts, want %d", f.engine.PartCount(), options.generations)
		}
		b.StartTimer()
		if err := f.engine.MergeWith(b.Context(), MergeOptions{Force: true}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if got := f.engine.PartCount(); got != 1 {
			b.Fatalf("merge left %d parts, want 1", got)
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
}

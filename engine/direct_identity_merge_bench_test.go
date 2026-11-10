package engine

import (
	"os"
	"slices"
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
		inputs := f.engine.PartPrefixes()
		expectedParts := f.prepareMerge(b)
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
		if got := f.engine.PartCount(); got != expectedParts {
			b.Fatalf("merge left %d parts, want %d", got, expectedParts)
		}
		for _, prefix := range f.engine.PartPrefixes() {
			if slices.Contains(inputs, prefix) {
				b.Fatalf("merge kept input %s", prefix)
			}
		}
		for _, host := range []int{0, options.hosts / 2, options.hosts - 1} {
			if err := f.fetchHost(b.Context(), host); err != nil {
				b.Fatal(err)
			}
		}
		f.reportDiskBytes(b)
		part := f.engine.parts[0]
		for _, candidate := range f.engine.parts[1:] {
			if candidate.index.seriesCount() > part.index.seriesCount() {
				part = candidate
			}
		}
		for _, name := range []string{colTs, colValue} {
			desc, ok := part.reader.ColumnDescByName(name)
			if !ok {
				b.Fatalf("missing column %s", name)
			}
			b.ReportMetric(float64(desc.Bytes), name+"-disk-B")
			b.ReportMetric(float64(desc.Sizing.NumGranules), name+"-granules")
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

package engine

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"testing"
)

// BenchmarkSeparateSeriesCPU isolates file-backed flush/merge over identical prebuilt CPU samples.
func BenchmarkSeparateSeriesCPU(b *testing.B) {
	options := cpuBenchOptions(b)
	options.disk, options.sync = true, true
	options.readCache = false
	options.generations = 2
	options.flushMemoryBytes = 2 << 30
	var seed, next *cpuStorageInput
	pprof.Do(b.Context(), pprof.Labels("phase", "input-generation"), func(context.Context) {
		seed = newCPUStorageInput(options, 0)
		next = seed.generation(1)
	})
	for _, phase := range []string{"flush-new", "flush-known", "merge"} {
		for _, separate := range []bool{false, true} {
			layout := "part-local"
			if separate {
				layout = "shared"
			}
			b.Run(phase+"/"+layout, func(b *testing.B) {
				benchmarkSeparateSeriesCPU(b, options, phase, layout, separate, seed, next)
			})
		}
	}
}

func benchmarkSeparateSeriesCPU(
	b *testing.B, options cpuFixtureOptions, phase, layout string, separate bool, seed, next *cpuStorageInput,
) {
	b.Helper()
	samples := len(seed.ids)
	if phase == "merge" {
		samples += len(next.ids)
	}
	b.ReportAllocs()
	b.SetBytes(int64(samples) * 16)
	b.Logf("hosts=%d series=%d points/series/generation=%d generations=2 file=true wal_sync=true phase=%s separate=%t",
		options.hosts, len(seed.identities), options.points, phase, separate)
	profiled := false
	for b.Loop() {
		b.StopTimer()
		f := newCPUFixture(b, b.Context(), options)
		f.engine.cfg.SeparateSeries = separate
		// Keep fixture construction from triggering unmeasured pressure flushes.
		f.engine.cfg.ResidentHeadBytes = math.MaxInt64
		if err := seed.appendTo(f.engine); err != nil {
			b.Fatal(err)
		}
		if phase != "flush-new" {
			if err := f.engine.Flush(b.Context()); err != nil {
				b.Fatal(err)
			}
			if got := f.engine.PartCount(); got != 1 {
				b.Fatalf("first flush parts=%d, want 1", got)
			}
			if err := next.appendTo(f.engine); err != nil {
				b.Fatal(err)
			}
		}
		if phase == "merge" {
			if err := f.engine.Flush(b.Context()); err != nil {
				b.Fatal(err)
			}
			if got := f.engine.PartCount(); got != 2 {
				b.Fatalf("merge inputs=%d, want 2", got)
			}
		}
		inputs := f.engine.PartPrefixes()
		runtime.GC()
		finishProfile := func() {}
		if !profiled {
			if prefix := os.Getenv("OTELDB_BENCH_SEPARATE_PROFILE"); prefix != "" {
				finish := cpuPhaseProfile(b, prefix+"."+phase+"."+layout)
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
		pprof.Do(b.Context(), pprof.Labels("phase", "storage", "operation", phase, "layout", layout), func(ctx context.Context) {
			b.StartTimer()
			var err error
			if phase == "merge" {
				err = f.engine.MergeWith(ctx, MergeOptions{Force: true})
			} else {
				err = f.engine.Flush(ctx)
			}
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
		})
		finishProfile()
		wantParts := 1
		if phase == "flush-known" {
			wantParts = 2
		}
		if got := f.engine.PartCount(); got != wantParts {
			b.Fatalf("output parts=%d, want %d", got, wantParts)
		}
		if phase == "merge" {
			for _, prefix := range f.engine.PartPrefixes() {
				if slices.Contains(inputs, prefix) {
					b.Fatalf("merge retained input part %s", prefix)
				}
			}
		}
		var storedRows int
		for _, p := range f.engine.parts {
			storedRows += p.rows()
		}
		wantRows := len(seed.ids)
		if phase != "flush-new" {
			wantRows += len(next.ids)
		}
		if storedRows != wantRows {
			b.Fatalf("stored rows=%d, want %d", storedRows, wantRows)
		}
		if got := f.engine.SeriesCount(); got != len(seed.identities) {
			b.Fatalf("active series=%d, want %d", got, len(seed.identities))
		}
		if phase == "flush-new" {
			f.options.generations = 1
		}
		for _, host := range []int{0, options.hosts / 2, options.hosts - 1} {
			if err := f.fetchHost(b.Context(), host); err != nil {
				b.Fatal(err)
			}
		}
		roots := 0
		if f.engine.seriesTable != nil {
			roots = len(f.engine.seriesTable.roots)
		}
		b.ReportMetric(float64(roots), "series-runs")
		b.ReportMetric(float64(f.engine.PartCount()), "numeric-parts")
		f.reportDiskBytes(b)
		if err := f.remove(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(len(seed.identities)), "series/op")
	b.ReportMetric(float64(samples), "samples/op")
}

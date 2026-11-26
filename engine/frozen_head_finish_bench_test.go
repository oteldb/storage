package engine

import (
	"context"
	"flag"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
)

type frozenHeadPhase string

const (
	buildFrozenHead   frozenHeadPhase = "build"
	publishFrozenHead frozenHeadPhase = "publish"
)

// BenchmarkFrozenHeadFinish isolates building and publishing a real WAL-sealed head.
func BenchmarkFrozenHeadFinish(b *testing.B) {
	profile := os.Getenv("OTELDB_BENCH_FROZEN_PROFILE")
	if profile != "" && (flag.Lookup("test.benchtime").Value.String() != "1x" ||
		flag.Lookup("test.count").Value.String() != "1") {
		b.Fatal("phase-only profiles require -benchtime=1x -count=1")
	}
	options := cpuFixtureOptions{
		hosts:            cpuBenchInt(b, "OTELDB_BENCH_CPU_HOSTS", 10000, 80000),
		points:           cpuBenchInt(b, "OTELDB_BENCH_CPU_POINTS", 1, 1024),
		flushMemoryBytes: int64(cpuBenchInt(b, "OTELDB_BENCH_FROZEN_FLUSH_MIB", 64, 448)) << 20,
		seriesCacheBytes: 4 << 20,
		disk:             true,
		separate:         true,
	}
	seed := newCPUStorageInput(options, 0)
	next := seed.generation(1)
	for _, history := range []string{"new-descriptors", "existing-descriptors"} {
		for _, phase := range []frozenHeadPhase{buildFrozenHead, publishFrozenHead} {
			b.Run(history+"/"+string(phase), func(b *testing.B) {
				path := profile
				if path != "" {
					path += "." + history + "." + string(phase)
				}
				var prior *cpuStorageInput
				if history == "existing-descriptors" {
					prior = seed
				}
				benchmarkFrozenHeadFinish(b, options, prior, next, phase, path)
			})
		}
	}
}

func benchmarkFrozenHeadFinish(b *testing.B, options cpuFixtureOptions, prior, input *cpuStorageInput,
	phase frozenHeadPhase, profile string,
) {
	b.Helper()
	rows := len(input.ids)
	b.ReportAllocs()
	if phase == buildFrozenHead {
		b.SetBytes(int64(rows) * SampleBytes)
	}
	var parts, priorRoots, newRoots, backendReads, fallbacks, builders int64
	var plans uint64
	for b.Loop() {
		b.StopTimer()
		f, generation := prepareFrozenFinish(b, options, prior, input)
		e := f.engine
		obs, metrics := obstest.New(b)
		e.cfg.Obs = obs
		beforeParts, beforeRoots := e.PartCount(), 0
		if e.seriesTable != nil {
			beforeRoots = len(e.seriesTable.roots)
		}
		if phase == publishFrozenHead {
			buildBenchmarkGeneration(b, b.Context(), e, generation, rows)
		}

		runtime.GC()
		f.counter.Reset()
		finishProfile := cpuPhaseProfile(b, profile)
		pprof.Do(b.Context(), pprof.Labels("frozen-head-phase", string(phase)), func(ctx context.Context) {
			b.StartTimer()
			if phase == buildFrozenHead {
				buildBenchmarkGeneration(b, ctx, e, generation, rows)
			} else {
				publishBenchmarkGeneration(b, ctx, e, rows)
			}
			b.StopTimer()
		})
		finishProfile()

		backendReads += f.counter.Bytes()
		priorRoots += int64(beforeRoots)
		newRoots += int64(len(e.seriesTable.roots) - beforeRoots)
		fallbacks += metrics.Counter("storage.flush.build.fallbacks")
		workers, count := metrics.IntHistogram("storage.flush.build.workers")
		builders, plans = builders+workers, plans+count
		if phase == buildFrozenHead {
			parts += int64(len(generation.parts))
			publishBenchmarkGeneration(b, b.Context(), e, rows)
		} else {
			parts += int64(e.PartCount() - beforeParts)
		}
		if err := f.remove(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(rows), "samples/op")
	b.ReportMetric(float64(len(input.identities)), "series/op")
	b.ReportMetric(float64(parts)/float64(b.N), "parts/op")
	b.ReportMetric(float64(priorRoots)/float64(b.N), "existing-roots/op")
	b.ReportMetric(float64(backendReads)/float64(b.N), "backend-read-B/op")
	if phase == buildFrozenHead {
		b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "samples/s")
		b.ReportMetric(float64(newRoots)/float64(b.N), "new-roots/op")
		b.ReportMetric(float64(fallbacks)/float64(b.N), "build-fallbacks/op")
		if plans != 0 {
			b.ReportMetric(float64(builders)/float64(plans), "builders/plan")
		}
	}
}

func prepareFrozenFinish(b *testing.B, options cpuFixtureOptions, prior, input *cpuStorageInput) (
	*cpuFixture, *pressureGeneration,
) {
	b.Helper()
	f := newCPUFixture(b, b.Context(), options)
	e := f.engine
	pool := memlimit.NewPool(8 << 30)
	e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
	e.cfg.ResidentHeadBytes = math.MaxInt64
	if prior != nil {
		seedFrozenFinishRoots(b, e, prior)
	}
	e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, 2
	if err := input.appendTo(e); err != nil {
		b.Fatal(err)
	}
	froze, err := e.rotateWithCommitOwner(b.Context(), true, false)
	if err != nil || !froze || len(e.pressureGenerations) != 1 {
		b.Fatalf("freeze prepared fixture: rotated=%t generations=%d error=%v", froze, len(e.pressureGenerations), err)
	}
	return f, e.pressureGenerations[0]
}

func seedFrozenFinishRoots(b *testing.B, e *Engine, prior *cpuStorageInput) {
	b.Helper()
	const roots = 8
	stride := max((len(prior.identities)+roots-1)/roots, 1)
	for lo := 0; lo < len(prior.identities); lo += stride {
		hi := min(lo+stride, len(prior.identities))
		from, to := lo*prior.points, hi*prior.points
		seed := cpuStorageInput{
			identities: prior.identities[lo:hi], ids: prior.ids[from:to], ts: prior.ts[from:to],
			values: prior.values[from:to], points: prior.points,
		}
		if err := seed.appendTo(e); err != nil {
			b.Fatal(err)
		}
		if err := e.Flush(b.Context()); err != nil {
			b.Fatalf("seed descriptor root: %v", err)
		}
	}
}

func buildBenchmarkGeneration(b *testing.B, ctx context.Context, e *Engine, g *pressureGeneration, want int) {
	b.Helper()
	r := e.buildFrozenGeneration(ctx, g)
	e.finishGenerationBuild(r)
	if r.err != nil || r.rows != want || len(r.parts) == 0 {
		b.Fatalf("build frozen generation: rows=%d want=%d parts=%d error=%v", r.rows, want, len(r.parts), r.err)
	}
}

func publishBenchmarkGeneration(b *testing.B, ctx context.Context, e *Engine, want int) {
	b.Helper()
	rows, _, err := e.publishReadyGenerations(ctx)
	if err != nil || rows != want || len(e.pressureGenerations) != 0 {
		b.Fatalf("publish frozen generation: rows=%d want=%d pending=%d error=%v", rows, want, len(e.pressureGenerations), err)
	}
}

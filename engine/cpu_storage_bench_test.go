package engine

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

type cpuStorageInput struct {
	identities []signal.Series
	ids        []signal.SeriesID
	ts         []int64
	values     []float64
	points     int
}

func newCPUStorageInput(options cpuFixtureOptions, generation int) *cpuStorageInput {
	input := &cpuStorageInput{
		identities: make([]signal.Series, options.hosts*len(cpuFixtureModes)),
		points:     options.points,
	}
	input.ids = make([]signal.SeriesID, len(input.identities)*input.points)
	for i := range input.identities {
		input.identities[i] = cpuMetricSeries(i)
		id := input.identities[i].Hash()
		for j := range input.points {
			input.ids[i*input.points+j] = id
		}
	}
	return input.generation(generation)
}

func (input *cpuStorageInput) generation(generation int) *cpuStorageInput {
	out := &cpuStorageInput{
		identities: input.identities, ids: input.ids, points: input.points,
		ts: make([]int64, len(input.ids)), values: make([]float64, len(input.ids)),
	}
	for i := range out.identities {
		for point := range out.points {
			j := i*out.points + point
			out.ts[j] = cpuFixtureBase + int64(generation*out.points+point)*cpuFixtureCadence
			out.values[j] = float64(i+generation*out.points+point) / 1000
		}
	}
	return out
}

func (input *cpuStorageInput) appendTo(engine *Engine) error {
	const batchRows = 4096
	batchSamples := max(1, batchRows/input.points) * input.points
	for offset := 0; offset < len(input.ids); offset += batchSamples {
		end := min(offset+batchSamples, len(input.ids))
		result, err := engine.AppendBatch(input.ids[offset:end], input.ts[offset:end], input.values[offset:end], nil,
			func(i int) signal.Series { return input.identities[(offset+i)/input.points] }, AppendLimits{})
		if err != nil {
			return err
		}
		if result.Accepted != end-offset {
			return errors.Errorf("append accepted=%d, want %d", result.Accepted, end-offset)
		}
	}
	return nil
}

type cpuStoragePhase string

const (
	cpuAppendCold       cpuStoragePhase = "append-cold"
	cpuAppendHeadWarm   cpuStoragePhase = "append-head-warm"
	cpuAppendAfterFlush cpuStoragePhase = "append-after-flush"
	cpuFlush            cpuStoragePhase = "flush"
)

func (phase cpuStoragePhase) prepare(ctx context.Context, f *cpuFixture, seed *cpuStorageInput) error {
	if phase == cpuAppendCold {
		return nil
	}
	if phase == cpuAppendHeadWarm || phase == cpuFlush {
		// Keep the bounded input resident so the case cannot silently become a pressure flush.
		f.engine.cfg.ResidentHeadBytes = math.MaxInt64
	}
	if err := seed.appendTo(f.engine); err != nil {
		return err
	}
	if phase == cpuAppendAfterFlush {
		return f.engine.Flush(ctx)
	}
	return nil
}

func (phase cpuStoragePhase) execute(ctx context.Context, f *cpuFixture, input *cpuStorageInput) error {
	if phase == cpuFlush {
		return f.engine.Flush(ctx)
	}
	return input.appendTo(f.engine)
}

func (f *cpuFixture) remove(ctx context.Context) error {
	if err := f.engine.Close(ctx); err != nil {
		return err
	}
	f.engine, f.counter = nil, nil
	return os.RemoveAll(f.root)
}

// BenchmarkDiskSeriesCPUStorage isolates append/flush from generated input, engine setup and seeding.
// Select one leaf with -benchtime=1x when profiling; see BENCHMARKS.md for phase-specific profiles.
func BenchmarkDiskSeriesCPUStorage(b *testing.B) {
	options := cpuBenchOptions(b)
	var seed, next *cpuStorageInput
	pprof.Do(b.Context(), pprof.Labels("phase", "input-generation"), func(context.Context) {
		seed = newCPUStorageInput(options, 0)
		next = seed.generation(1)
	})
	for _, phase := range []cpuStoragePhase{cpuAppendCold, cpuAppendHeadWarm, cpuAppendAfterFlush, cpuFlush} {
		b.Run(string(phase), func(b *testing.B) {
			b.Logf("hosts=%d series=%d points/series=%d disk_series=%t synced_WAL=%t",
				options.hosts, len(seed.identities), options.points, options.disk, options.sync)
			b.ReportAllocs()
			b.SetBytes(int64(len(seed.ids)) * SampleBytes)
			profiled := false
			for b.Loop() {
				b.StopTimer()
				var f *cpuFixture
				pprof.Do(b.Context(), pprof.Labels("phase", "engine-setup"), func(ctx context.Context) {
					f = newCPUFixture(b, ctx, options)
				})
				pprof.Do(b.Context(), pprof.Labels("phase", "seed"), func(ctx context.Context) {
					if err := phase.prepare(ctx, f, seed); err != nil {
						b.Fatal(err)
					}
					runtime.GC()
				})
				finishProfile := func() {}
				if !profiled {
					prefix := os.Getenv("OTELDB_BENCH_CPU_PROFILE")
					if prefix != "" {
						finishProfile = cpuPhaseProfile(b, prefix+"."+string(phase))
					}
					profiled = true
				}
				input := next
				if phase == cpuAppendCold {
					input = seed
				}
				pprof.Do(b.Context(), pprof.Labels("phase", "storage", "operation", string(phase)), func(ctx context.Context) {
					b.StartTimer()
					if err := phase.execute(ctx, f, input); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
				})
				finishProfile()
				if got := f.engine.Stats().Series; got != int64(len(seed.identities)) {
					b.Fatalf("series=%d, want %d", got, len(seed.identities))
				}
				pprof.Do(b.Context(), pprof.Labels("phase", "cleanup"), func(ctx context.Context) {
					if err := f.remove(ctx); err != nil {
						b.Fatal(err)
					}
				})
				b.StartTimer()
			}
			b.ReportMetric(float64(len(seed.ids)), "samples/op")
			b.ReportMetric(float64(len(seed.identities)), "series/op")
		})
	}
}

// BenchmarkDiskSeriesCPUInputGeneration measures the identity hashing and vector generation alone.
func BenchmarkDiskSeriesCPUInputGeneration(b *testing.B) {
	options := cpuBenchOptions(b)
	b.ReportAllocs()
	pprof.Do(b.Context(), pprof.Labels("phase", "input-generation"), func(context.Context) {
		for b.Loop() {
			runtime.KeepAlive(newCPUStorageInput(options, 0))
		}
	})
}

package engine

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
)

// BenchmarkWritableHeads compares fixed total budgets and includes stable routing and gathers.
// Seed, input generation, reopen and cleanup are excluded; each round appends 80k CPU series.
func BenchmarkWritableHeads(b *testing.B) {
	for _, phase := range []string{"cold", "historical", "resident"} {
		for _, prepared := range []bool{false, true} {
			for _, heads := range []int{1, 2, 4, 8, 16} {
				b.Run(fmt.Sprintf("%s/prepared=%t/heads=%d", phase, prepared, heads), func(b *testing.B) {
					benchmarkWritableHeads(b, phase, heads, prepared, "base")
				})
			}
		}
	}
}

func BenchmarkHeadDispatch(b *testing.B) {
	for _, phase := range []string{"cold", "historical", "resident"} {
		for _, variant := range []string{"base", "shared", "queued", "combined"} {
			for _, heads := range []int{1, 4, 8} {
				b.Run(fmt.Sprintf("%s/%s/heads=%d", phase, variant, heads), func(b *testing.B) {
					benchmarkWritableHeads(b, phase, heads, true, variant)
				})
			}
		}
	}
}

func benchmarkWritableHeads(b *testing.B, phase string, heads int, prepared bool, variant string) {
	b.Helper()
	const points, writers = 4096, 8
	seedInput := newCPUStorageInput(cpuFixtureOptions{hosts: 10000, points: 1}, 0)
	input := seedInput.generation(1)
	f := newWritableHeads(b, heads)
	f.prepared = prepared
	if variant != "base" {
		if err := f.close(); err != nil {
			b.Fatal(err)
		}
		f.sharedMetadata = variant == "shared" || variant == "combined"
		f.queued = variant == "queued" || variant == "combined"
		if err := f.open(b.Context(), false); err != nil {
			b.Fatal(err)
		}
	}
	var seedRoute headRouteBatch
	appendInput := func(ctx context.Context, route *headRouteBatch, input *cpuStorageInput, offset int) (AppendResult, error) {
		hi := min(offset+points, len(input.ids))
		return route.append(ctx, f, input.ids[offset:hi], input.ts[offset:hi], input.values[offset:hi], nil,
			func(i int) signal.Series { return input.identities[offset+i] })
	}
	seed := func() {
		for offset := 0; offset < len(input.ids); offset += points {
			if _, err := appendInput(b.Context(), &seedRoute, seedInput, offset); err != nil {
				b.Fatal(err)
			}
		}
	}
	if phase != "cold" {
		seed()
		if err := f.flush(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	var seedParts int
	for _, h := range f.heads {
		seedParts += h.PartCount()
	}
	routes := make([]headRouteBatch, writers)
	latencies := make([][]int64, writers)
	for worker := range routes {
		// Routing scratch is reusable caller state, not fixture generation in timed work.
		if heads > 1 {
			routes[worker] = headRouteBatch{counts: make([]int, heads), offsets: make([]int, heads+1), cursors: make([]int, heads),
				refs: make([]int, points), ids: make([]signal.SeriesID, points), ts: make([]int64, points), values: make([]float64, points)}
		}
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(input.ids)) * SampleBytes)
	b.ResetTimer()
	profiled := false
	for b.Loop() {
		b.StopTimer()
		if err := f.resetFixture(b.Context()); err != nil {
			b.Fatal(err)
		}
		if phase == "resident" {
			seed()
			var resident int
			for _, h := range f.heads {
				resident += h.head.series.Len()
			}
			if resident != len(input.identities) {
				b.Fatalf("resident identities=%d, want %d", resident, len(input.identities))
			}
		}
		runtime.GC()
		finishProfile := func() {}
		if !profiled {
			finishProfile = cpuPhaseProfile(b, os.Getenv("OTELDB_BENCH_WRITABLE_HEAD_PROFILE"))
			profiled = true
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		pprof.Do(b.Context(), pprof.Labels("phase", "storage"), func(ctx context.Context) {
			for worker := range writers {
				wg.Go(func() {
					<-start
					for offset := worker * points; offset < len(input.ids); offset += writers * points {
						begin := time.Now()
						r, err := appendInput(ctx, &routes[worker], input, offset)
						latencies[worker] = append(latencies[worker], time.Since(begin).Nanoseconds())
						if err != nil || r.Accepted != min(points, len(input.ids)-offset) {
							b.Errorf("append: %+v, %v", r, err)
							return
						}
					}
				})
			}
		})
		b.StartTimer()
		close(start)
		wg.Wait()
		b.StopTimer()
		finishProfile()
		b.StartTimer()
	}
	b.StopTimer()
	var all []int64
	for _, row := range latencies {
		all = append(all, row...)
	}
	slices.Sort(all)
	if len(all) > 0 {
		b.ReportMetric(float64(all[(95*len(all)+99)/100-1]), "p95-call-ns")
	}
	b.ReportMetric(float64(len(input.ids)), "samples/op")
	b.ReportMetric(float64(seedParts), "seed-parts")
	b.ReportMetric(float64(len(input.ids))/b.Elapsed().Seconds()*float64(b.N), "samples/s")
}

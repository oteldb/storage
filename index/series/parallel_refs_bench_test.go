package series

import (
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"

	"github.com/oteldb/storage/signal"
)

var parallelRefsSink uint64

// BenchmarkParallelSeries fixes the total host count while varying worker count.
// Lookups share an immutable index; writers own independent indexes and share templates.
func BenchmarkParallelSeries(b *testing.B) {
	hosts := 10000
	if s := os.Getenv("OTELDB_BENCH_SERIES_HOSTS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 10000 {
			b.Fatal("invalid hosts")
		}
		hosts = n
	}
	repeats := 6
	if s := os.Getenv("OTELDB_BENCH_PACKED_REPEATS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 10 {
			b.Fatal("invalid repeats")
		}
		repeats = n
	}
	fixtureCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "fixture"))
	pprof.SetGoroutineLabels(fixtureCtx)
	defer pprof.SetGoroutineLabels(b.Context())
	fixture := newSeriesOnlyFixture(b, hosts)
	count := fixture.count()
	ids := make([]signal.SeriesID, count)
	buf := make([]byte, 0, 4096)
	source := NewWithAttributeTemplates(nil, exporterPool("dimensions", "cold"))
	var expectedChecksum uint64
	for offset := 0; offset < count; offset += seriesOnlyBatchSize {
		batch := fixture.prepareSeriesBatch(offset)
		for i := range batch {
			buf = batch[i].AppendHashInput(buf[:0])
			id := signal.HashBytes(buf)
			ids[offset+i] = id
			source.AddEntry(Entry{ID: id, Series: batch[i]})
			expectedChecksum ^= id.Hi ^ id.Lo
		}
	}
	shuffled := append([]signal.SeriesID(nil), ids...)
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	runtime.GC()
	for repeat := range repeats {
		workersList := []int{1, 2, 4, 8}
		if repeat%2 != 0 {
			workersList = []int{8, 4, 2, 1}
		}
		for _, workers := range workersList {
			for _, phase := range []string{"lookup-ordered", "lookup-shuffled", "register-sharded"} {
				b.Run("hosts="+strconv.Itoa(hosts)+"/repeat="+strconv.Itoa(repeat)+"/workers="+strconv.Itoa(workers)+"/"+phase, func(b *testing.B) {
					workCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", phase, "workers", strconv.Itoa(workers)))
					var inputs []*seriesOnlyFixture
					if phase == "register-sharded" {
						for range workers {
							inputs = append(inputs, newSeriesOnlyFixture(b, hosts))
						}
					}
					inputIDs := ids
					if phase == "lookup-shuffled" {
						inputIDs = shuffled
					}
					var indexes []*Index
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						pprof.SetGoroutineLabels(fixtureCtx)
						indexes = nil
						if phase == "register-sharded" {
							runtime.GC()
							pool := exporterPool("dimensions", "cold")
							for range workers {
								indexes = append(indexes, NewWithAttributeTemplates(nil, pool))
							}
						}
						sums := make([]uint64, workers)
						checksums := make([]uint64, workers)
						failed := make([]bool, workers)
						start := make(chan struct{})
						var wg sync.WaitGroup
						for worker := range workers {
							wg.Go(func() {
								pprof.SetGoroutineLabels(workCtx)
								<-start
								first, last := count*worker/workers, count*(worker+1)/workers
								var sum, checksum uint64
								if phase == "register-sharded" {
									ix := indexes[worker]
									f := inputs[worker]
									for offset := first; offset < last; offset += seriesOnlyBatchSize {
										batch := f.prepareSeriesBatch(offset)
										batch = batch[:min(len(batch), last-offset)]
										for i := range batch {
											id := ids[offset+i]
											got := ix.AddEntry(Entry{ID: id, Series: batch[i]})
											if got != id {
												failed[worker] = true
												return
											}
											checksum ^= got.Hi ^ got.Lo
										}
									}
									sum = uint64(ix.Len())
								} else {
									for _, id := range inputIDs[first:last] {
										ref, ok := source.Ref(id)
										if !ok {
											failed[worker] = true
											return
										}
										sum += uint64(ref)
										checksum ^= id.Hi ^ id.Lo
									}
								}
								sums[worker], checksums[worker] = sum, checksum
							})
						}
						b.StartTimer()
						close(start)
						wg.Wait()
						b.StopTimer()
						pprof.SetGoroutineLabels(fixtureCtx)
						var sum, checksum uint64
						for worker := range workers {
							if failed[worker] {
								b.Fatal("identity/reference mismatch")
							}
							sum += sums[worker]
							checksum ^= checksums[worker]
						}
						want := uint64(count) * uint64(count-1) / 2
						if phase == "register-sharded" {
							want = uint64(count)
						}
						if sum != want || checksum != expectedChecksum {
							b.Fatal("wrong identity/reference result")
						}
						parallelRefsSink = checksum
						b.StartTimer()
					}
					b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "series/s")
					var charged int64
					for _, ix := range indexes {
						charged += ix.SizeBytes()
					}
					if phase == "register-sharded" {
						b.ReportMetric(float64(charged), "registry-B")
					}
					runtime.KeepAlive(source)
					runtime.KeepAlive(indexes)
					runtime.KeepAlive(inputs)
				})
			}
		}
	}
}

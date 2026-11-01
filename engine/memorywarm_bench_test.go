package engine

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkMemoryWarmAppend(b *testing.B) {
	for _, points := range []int{64, 512, 4096} {
		for _, writers := range []int{1, 8} {
			for _, immutable := range []bool{false, true} {
				b.Run(fmt.Sprintf("points=%d/writers=%d/immutable=%t", points, writers, immutable), func(b *testing.B) {
					benchmarkMemoryWarmAppend(b, points, writers, immutable)
				})
			}
		}
	}
}

func benchmarkMemoryWarmAppend(b *testing.B, points, writers int, immutable bool) {
	b.Helper()
	w, err := wal.Create(b.TempDir(), 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	e := New(Config{WAL: w})
	b.Cleanup(func() {
		e.stopMemoryWork()
		if e.readView != nil {
			e.readView.release()
		}
		_ = w.Close()
	})
	ids := make([][]signal.SeriesID, writers)
	latencies := make([][]int64, writers)
	ts, values := make([]int64, points), make([]float64, points)
	for worker := range writers {
		ids[worker] = make([]signal.SeriesID, points)
		latencies[worker] = make([]int64, 0, (b.N+writers-1)/writers)
		for i := range ids[worker] {
			s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
				Key: []byte("instance"), Value: signal.IntValue(int64(worker*points + i)),
			})}
			e.head.registerSeries(s)
			ids[worker][i] = s.Hash()
		}
	}
	if immutable {
		e.initMemoryPublication()
	}
	materialize := func(int) signal.Series { panic("warm identity materialized") }
	for worker := range writers {
		if _, err := e.AppendBatch(ids[worker], ts, values, nil, materialize, AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(points) * SampleBytes)
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for worker := range writers {
		wg.Go(func() {
			for i := worker; i < b.N; i += writers {
				start := time.Now()
				_, err := e.AppendBatch(ids[worker], ts, values, nil, materialize, AppendLimits{})
				latencies[worker] = append(latencies[worker], time.Since(start).Nanoseconds())
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	all := make([]int64, 0, b.N)
	for _, times := range latencies {
		all = append(all, times...)
	}
	slices.Sort(all)
	if len(all) > 0 {
		b.ReportMetric(float64(all[(len(all)*95+99)/100-1]), "p95-ack-ns")
	}
}

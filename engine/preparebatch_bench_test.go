package engine

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkAppendPreparation(b *testing.B) {
	for _, points := range []int{512, 4096} {
		for _, repeat := range []int{1, 8} {
			b.Run(fmt.Sprintf("points=%d/repeat=%d", points, repeat), func(b *testing.B) {
				w, err := wal.Create(b.TempDir(), 1<<30)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = w.Close() })
				e := New(Config{WAL: w, OOOWindow: 1000})
				workers := runtime.GOMAXPROCS(0)
				distinct := points / repeat
				allIDs := make([][]signal.SeriesID, workers)
				ts, values := make([]int64, points), make([]float64, points)
				for i := range ts {
					ts[i], values[i] = 1, 1
				}
				for worker := range workers {
					ids := make([]signal.SeriesID, points)
					idents := make([]signal.Series, distinct)
					for i := range idents {
						idents[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
							Key: []byte("instance"), Value: signal.IntValue(int64(worker*distinct + i)),
						})}
					}
					for i := range ids {
						ids[i] = idents[i%distinct].Hash()
					}
					allIDs[worker] = ids
					_, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return idents[i%distinct] }, AppendLimits{})
					if err != nil {
						b.Fatal(err)
					}
				}
				materialize := func(int) signal.Series { panic("warm identity materialized") }
				var warm sync.WaitGroup
				for worker := range workers {
					warm.Go(func() {
						for range 8 {
							if _, err := e.AppendBatch(allIDs[worker], ts, values, nil, materialize, AppendLimits{}); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				warm.Wait()
				// Exclude head slice growth so the comparison isolates staging and writer contention.
				additional := ((b.N + workers - 1) / workers) * repeat
				for _, buf := range e.head.samples {
					buf.ts = slices.Grow(buf.ts, additional)
					buf.values = slices.Grow(buf.values, additional)
				}
				b.SetBytes(int64(points) * SampleBytes)
				b.ReportAllocs()
				b.ResetTimer()
				var wg sync.WaitGroup
				for worker := range workers {
					wg.Go(func() {
						for i := worker; i < b.N; i += workers {
							if _, err := e.AppendBatch(allIDs[worker], ts, values, nil, materialize, AppendLimits{}); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				wg.Wait()
			})
		}
	}
}

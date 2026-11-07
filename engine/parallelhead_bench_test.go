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

func BenchmarkKnownSeriesPublication(b *testing.B) {
	for _, points := range []int{64, 4096} {
		for _, writers := range []int{1, 8} {
			for _, fresh := range []bool{false, true} {
				b.Run(fmt.Sprintf("points=%d/writers=%d/fresh=%t", points, writers, fresh), func(b *testing.B) {
					benchmarkKnownPublication(b, points, writers, fresh)
				})
			}
		}
	}
}

func benchmarkKnownPublication(b *testing.B, points, writers int, fresh bool) {
	b.Helper()
	w, err := wal.Create(b.TempDir(), 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	e := New(Config{WAL: w})
	warmRows := writers * points
	rows := warmRows
	if fresh {
		rows += b.N * points
	}
	ids := make([]signal.SeriesID, rows)
	for i := range ids {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("instance"), Value: signal.IntValue(int64(i))})}
		e.head.registerSeries(s)
		ids[i] = s.Hash()
	}
	ts, values := make([]int64, points), make([]float64, points)
	for i := range ts {
		ts[i], values[i] = 1, 1
	}
	materialize := func(int) signal.Series { panic("known identity materialized") }
	limits := AppendLimits{MaxInFlightBytes: 128 << 20}
	for worker := range writers {
		_, err := e.AppendBatch(ids[worker*points:(worker+1)*points], ts, values, nil, materialize, limits)
		if err != nil {
			b.Fatal(err)
		}
	}
	extra := 4 + (maxPreparedBatches+writers-1)/writers
	if !fresh {
		extra += (b.N + writers - 1) / writers
	}
	for _, buf := range e.head.samples {
		buf.ts = slices.Grow(buf.ts, extra)
		buf.values = slices.Grow(buf.values, extra)
	}
	prepared := make([]*preparedBatch, maxPreparedBatches)
	for i := range prepared {
		p := metricBatchPreparations.acquire()
		if p == nil {
			b.Fatal("preparation pool busy")
		}
		prepared[i] = p
		worker := i % writers
		batchIDs := ids[worker*points : (worker+1)*points]
		p.prepare(batchIDs)
		if _, err := e.appendPrepared(batchIDs, ts, values, nil, materialize, limits, p); err != nil {
			b.Fatal(err)
		}
	}
	for _, p := range prepared {
		metricBatchPreparations.release(p)
	}
	start := make(chan struct{})
	var warm sync.WaitGroup
	for worker := range writers {
		warm.Go(func() {
			<-start
			for range 4 {
				if _, err := e.AppendBatch(ids[worker*points:(worker+1)*points], ts, values, nil, materialize, limits); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	close(start)
	warm.Wait()
	latencies := make([][]int64, writers)
	for worker := range writers {
		latencies[worker] = make([]int64, 0, (b.N+writers-1)/writers)
	}
	b.ReportAllocs()
	b.SetBytes(int64(points) * SampleBytes)
	b.ResetTimer()
	var wg sync.WaitGroup
	for worker := range writers {
		wg.Go(func() {
			for i := worker; i < b.N; i += writers {
				offset := worker * points
				if fresh {
					offset = warmRows + i*points
				}
				started := time.Now()
				res, err := e.AppendBatch(ids[offset:offset+points], ts, values, nil, materialize, limits)
				latencies[worker] = append(latencies[worker], time.Since(started).Nanoseconds())
				if err != nil || res.Accepted != points {
					b.Errorf("append: %+v, %v", res, err)
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
		b.ReportMetric(float64(all[(95*len(all)+99)/100-1]), "p95-call-ns")
	}
}

package engine

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkSampleBufferGenerations(b *testing.B) {
	for _, points := range []int{1, 2, 4, 8, 64} {
		appends := []int{1}
		if points == 64 {
			appends = nil
		}
		if points != 1 {
			appends = append(appends, points)
		}
		for _, perAppend := range appends {
			b.Run(fmt.Sprintf("series=10000/points=%d/append=%d", points, perAppend), func(b *testing.B) {
				const count = 10000
				h := newHead()
				batch := &walBatch{order: make([]signal.SeriesID, count), accs: make([]walSeriesAcc, count)}
				ts, values, sf := make([]int64, points), make([]float64, points), make([]float64, points)
				for i := range ts {
					ts[i], values[i], sf[i] = int64(i), float64(i), 1
				}
				for i := range count {
					s := benchSeries(i)
					h.registerSeries(s)
					batch.order[i] = s.Hash()
					batch.accs[i] = walSeriesAcc{knownRef: true, identityRef: uint32(i), ts: ts[:perAppend], values: values[:perAppend], sf: sf[:perAppend]}
				}
				b.ReportAllocs()
				b.SetBytes(count * int64(points) * SampleBytes)
				b.ResetTimer()
				var epoch int64
				for b.Loop() {
					for i := range ts {
						ts[i] = epoch + int64(i)
					}
					for point := 0; point < points; point += perAppend {
						if perAppend == 1 {
							ts[0] = epoch + int64(point)
						}
						batch.apply(h)
					}
					h.detach()
					h.frozenMemory.release()
					h.detachedBytes = 0
					epoch += int64(points)
				}
			})
		}
	}
}

func BenchmarkSampleBufferFileWALFlush(b *testing.B) {
	for _, points := range []int{1, 4} {
		b.Run(fmt.Sprintf("series=10000/points=%d", points), func(b *testing.B) {
			const count = 10000
			dir := b.TempDir()
			files, err := backendfile.New(filepath.Join(dir, "parts"))
			if err != nil {
				b.Fatal(err)
			}
			w, err := wal.Create(filepath.Join(dir, "wal"), 0)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = w.Close() })
			e := New(Config{Backend: files, WAL: w, Prefix: "metrics"})
			identities := make([]signal.Series, count)
			ids := make([]signal.SeriesID, count)
			for i := range identities {
				identities[i] = benchSeries(i)
				ids[i] = identities[i].Hash()
				e.head.registerSeries(identities[i])
			}
			ts, values := make([]int64, count), make([]float64, count)
			materialize := func(int) signal.Series { panic("registered series materialized") }
			b.ReportAllocs()
			b.SetBytes(count * int64(points) * SampleBytes)
			b.ResetTimer()
			for b.Loop() {
				for point := range points {
					for i := range ts {
						ts[i], values[i] = int64(point), float64(i)
					}
					if _, err := e.AppendBatch(ids, ts, values, nil, materialize, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
				}
				if err := e.Flush(b.Context()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := e.Reset(b.Context()); err != nil {
					b.Fatal(err)
				}
				for i := range identities {
					e.head.registerSeries(identities[i])
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkSampleBufferRetainedBytes(b *testing.B) {
	for _, points := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("points=%d", points), func(b *testing.B) {
			const count = 10000
			for b.Loop() {
				h := newHead()
				batch := &walBatch{order: make([]signal.SeriesID, count), accs: make([]walSeriesAcc, count)}
				ts, values, sf := []int64{0}, []float64{1}, []float64{1}
				for i := range count {
					s := benchSeries(i)
					h.registerSeries(s)
					batch.order[i] = s.Hash()
					batch.accs[i] = walSeriesAcc{knownRef: true, identityRef: uint32(i), ts: ts, values: values, sf: sf}
				}
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				for point := range points {
					ts[0] = int64(point)
					batch.apply(h)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained-B")
				b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects)), "retained-objs")
				runtime.KeepAlive(batch)
				runtime.KeepAlive(h)
			}
		})
	}
}

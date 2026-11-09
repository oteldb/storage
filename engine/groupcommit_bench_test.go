package engine

import (
	"fmt"
	"io/fs"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkGroupCommit(b *testing.B) {
	for _, points := range []int{4096, 8192, 16384} {
		for _, writers := range []int{1, 4, 8} {
			for _, syncWrites := range []bool{false, true} {
				b.Run(fmt.Sprintf("points=%d/writers=%d/sync=%t", points, writers, syncWrites), func(b *testing.B) {
					benchmarkGroupCommit(b, points, writers, syncWrites)
				})
			}
		}
	}
}

func benchmarkGroupCommit(b *testing.B, points, writers int, syncWrites bool) {
	b.Helper()
	fsys, err := vfs.OpenRoot(b.TempDir(), 0o750)
	if err != nil {
		b.Fatal(err)
	}
	counts := &countedWALFS{FS: fsys}
	w, err := wal.CreateFS(counts, 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	w.SetSync(syncWrites)
	e := New(Config{WAL: w})
	warmRows := writers * points
	rows := warmRows

	ids := make([]signal.SeriesID, rows)
	for i := range ids {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("instance"), Value: signal.IntValue(int64(i))})}
		e.head.registerSeries(s)
		ids[i] = s.Hash()
	}
	ts, values := make([]int64, points), make([]float64, points)
	for i := range ts {
		ts[i], values[i] = 1_794_220_000_000_000_000, 1
	}
	materialize := func(int) signal.Series { panic("known identity materialized") }
	limits := AppendLimits{MaxInFlightBytes: 1 << 30}
	for worker := range writers {
		_, err := e.AppendBatch(ids[worker*points:(worker+1)*points], ts, values, nil, materialize, limits)
		if err != nil {
			b.Fatal(err)
		}
	}
	extra := 4 + (maxPreparedBatches+writers-1)/writers
	extra += (b.N + writers - 1) / writers
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
	workerTS := make([][]int64, writers)
	for worker := range writers {
		latencies[worker] = make([]int64, 0, (b.N+writers-1)/writers)
		workerTS[worker] = slices.Clone(ts)
	}
	b.ReportAllocs()
	b.SetBytes(int64(points) * SampleBytes)
	counts.writes, counts.fsyncs = 0, 0
	b.ResetTimer()
	var wg sync.WaitGroup
	for worker := range writers {
		wg.Go(func() {
			for i := worker; i < b.N; i += writers {
				offset := worker * points
				timestamps := workerTS[worker]
				for j := range timestamps {
					timestamps[j] += int64(15 * time.Second)
				}
				started := time.Now()
				res, err := e.AppendBatch(ids[offset:offset+points], timestamps, values, nil, materialize, limits)
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
	expectedNewest := ts[0] + int64((b.N+writers-1)/writers)*int64(15*time.Second)
	if e.head.newest != expectedNewest {
		b.Fatalf("newest timestamp: got %d, want %d", e.head.newest, expectedNewest)
	}
	b.ReportMetric(float64(counts.writes)/float64(b.N), "writes/op")
	b.ReportMetric(float64(counts.fsyncs)/float64(b.N), "fsyncs/op")
	all := make([]int64, 0, b.N)
	for _, times := range latencies {
		all = append(all, times...)
	}
	slices.Sort(all)
	if len(all) > 0 {
		b.ReportMetric(float64(all[(95*len(all)+99)/100-1]), "p95-call-ns")
	}
}

type countedWALFS struct {
	vfs.FS

	writes, fsyncs int64
}

func (f *countedWALFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &countedWALFile{File: file, counts: f}, nil
}

type countedWALFile struct {
	vfs.File

	counts *countedWALFS
}

func (f *countedWALFile) Write(p []byte) (int, error) {
	f.counts.writes++
	return f.File.Write(p)
}

func (f *countedWALFile) Sync() error {
	f.counts.fsyncs++
	return f.File.Sync()
}

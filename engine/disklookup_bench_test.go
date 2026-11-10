package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// BenchmarkHistoricalSeriesPublication excludes input, seed and fixture-reopen work.
// Every measured round resolves evicted identities from file-backed parts with unsynced WAL.
func BenchmarkHistoricalSeriesPublication(b *testing.B) {
	for _, points := range []int{1024, 4096} {
		for _, writers := range []int{1, 8} {
			b.Run(fmt.Sprintf("points=%d/writers=%d", points, writers), func(b *testing.B) {
				benchmarkHistoricalPublication(b, points, writers, false)
			})
		}
	}
}

func BenchmarkResidentDiskSeriesPublication(b *testing.B) {
	for _, writers := range []int{1, 8} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			benchmarkHistoricalPublication(b, 4096, writers, true)
		})
	}
}

func benchmarkHistoricalPublication(b *testing.B, points, writers int, resident bool) {
	b.Helper()
	f := newCPUFixture(b, b.Context(), cpuFixtureOptions{disk: true, sync: false})
	e := f.engine
	identities := make([]signal.Series, points*writers)
	ids := make([]signal.SeriesID, len(identities))
	for i := range ids {
		identities[i] = cpuMetricSeries(i)
		ids[i] = identities[i].Hash()
	}
	ts, values := make([]int64, points), make([]float64, points)
	for i := range ts {
		ts[i], values[i] = cpuFixtureBase, 1
	}
	appendBatch := func(worker int) (AppendResult, error) {
		offset := worker * points
		return e.AppendBatch(ids[offset:offset+points], ts, values, nil,
			func(i int) signal.Series { return identities[offset+i] }, AppendLimits{})
	}
	for worker := range writers {
		if _, err := appendBatch(worker); err != nil {
			b.Fatal(err)
		}
	}
	if err := e.Flush(b.Context()); err != nil {
		b.Fatal(err)
	}
	cfg := e.cfg
	latencies := make([][]int64, writers)
	b.ReportAllocs()
	b.SetBytes(int64(len(ids)) * SampleBytes)
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		if err := e.CloseWAL(); err != nil {
			b.Fatal(err)
		}
		walDir := filepath.Join(f.root, "wal")
		if err := os.RemoveAll(walDir); err != nil {
			b.Fatal(err)
		}
		w, err := wal.Create(walDir, 0)
		if err != nil {
			b.Fatal(err)
		}
		w.SetSync(false)
		cfg.WAL = w
		e = New(cfg)
		f.engine = e
		if err := e.LoadParts(b.Context()); err != nil {
			b.Fatal(err)
		}
		if resident {
			for worker := range writers {
				if _, err := appendBatch(worker); err != nil {
					b.Fatal(err)
				}
			}
		}
		if !resident && e.head.series.Len() != 0 {
			b.Fatal("historical fixture retained resident identities")
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for worker := range writers {
			wg.Go(func() {
				<-start
				begin := time.Now()
				r, err := appendBatch(worker)
				latencies[worker] = append(latencies[worker], time.Since(begin).Nanoseconds())
				if err != nil || r.Accepted != points {
					b.Errorf("append: %+v, %v", r, err)
				}
			})
		}
		b.StartTimer()
		close(start)
		wg.Wait()
		b.StopTimer()
		b.StartTimer()
	}
	b.StopTimer()
	all := make([]int64, 0, b.N*writers)
	for _, times := range latencies {
		all = append(all, times...)
	}
	slices.Sort(all)
	if len(all) > 0 {
		b.ReportMetric(float64(all[(95*len(all)+99)/100-1]), "p95-call-ns")
	}
	b.ReportMetric(float64(len(ids)), "samples/op")
}

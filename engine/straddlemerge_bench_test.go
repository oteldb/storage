package engine_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// peakHeap samples the live heap until stop is called, returning the highest reading.
func peakHeap() (stop func() uint64) {
	var (
		peak uint64
		wg   sync.WaitGroup
		done = make(chan struct{})
	)

	wg.Go(func() {
		var ms runtime.MemStats

		for {
			runtime.ReadMemStats(&ms)
			peak = max(peak, ms.HeapAlloc)

			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
		}
	})

	return func() uint64 {
		close(done)
		wg.Wait()

		return peak
	}
}

// BenchmarkMergeStraddlers17Days merges one straddler batch whose samples cover 17 days: 16 parts of
// 64 series sampled hourly. It reports what the merge reads from the backend and its peak live heap,
// the costs a per-day pass structure multiplies by the days it writes.
func BenchmarkMergeStraddlers17Days(b *testing.B) {
	const (
		parts  = 16
		series = 64
		days   = 17
		hour   = int64(time.Hour)
	)

	ss := make([]signal.Series, series)
	for i := range ss {
		ss[i] = mkSeries("job", fmt.Sprintf("s%d", i))
	}

	var (
		read, peak     uint64
		rows           int
		writers, limit int64
	)

	defer engine.SetMergeResidentObserver(func(p, _, l int64) { writers, limit = max(writers, p), l })()

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()

		be := backendtest.NewByteCounter(backend.Memory())
		e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

		rows = 0

		for p := range parts {
			for i := range ss {
				for h := range int64(days * 24) {
					if _, err := e.Append(ss[i], h*hour+int64(p)*int64(time.Minute), float64(h)); err != nil {
						b.Fatal(err)
					}

					rows++
				}
			}

			if err := e.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		}

		be.Reset()
		runtime.GC()

		stop := peakHeap()

		b.StartTimer()

		if err := e.Merge(ctx, 0); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()

		peak = max(peak, stop())
		read = uint64(be.Bytes())

		if e.PartCount() < days {
			b.Fatalf("%d parts, want one per day", e.PartCount())
		}

		b.StartTimer()
	}

	b.ReportMetric(float64(rows), "rows")
	b.ReportMetric(float64(read), "read-B")
	b.ReportMetric(float64(peak), "peak-heap-B")
	b.ReportMetric(float64(writers), "writers-peak-B")
	b.ReportMetric(float64(limit), "resident-limit-B")
}

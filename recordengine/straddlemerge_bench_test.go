package recordengine_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/recordengine"
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

// BenchmarkMergeStraddlers17Days merges one straddler batch whose records cover 17 days: 16 parts of
// 64 streams logging hourly. It reports what the merge reads from the backend and its peak live heap,
// the costs a per-day pass structure multiplies by the days it writes.
func BenchmarkMergeStraddlers17Days(b *testing.B) {
	const (
		parts   = 16
		streams = 64
		days    = 17
		hour    = int64(time.Hour)
	)

	var (
		read, peak uint64
		rows       int
	)

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()

		be := backendtest.NewByteCounter(backend.Memory())
		e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "t/recs"})

		rows = 0

		for p := range parts {
			for s := range streams {
				recs := make([]rrec, 0, days*24)
				for h := range int64(days * 24) {
					recs = append(recs, rrec{ts: h*hour + int64(p)*int64(time.Minute), body: fmt.Sprintf("record %d", h)})
				}

				if _, err := e.AppendBatch(mkBatch(fmt.Sprintf("svc%d", s), recs...), recordengine.AppendLimits{}); err != nil {
					b.Fatal(err)
				}

				rows += len(recs)
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

		if len(e.Parts()) < days {
			b.Fatalf("%d parts, want one per day", len(e.Parts()))
		}

		b.StartTimer()
	}

	b.ReportMetric(float64(rows), "rows")
	b.ReportMetric(float64(read), "read-B")
	b.ReportMetric(float64(peak), "peak-heap-B")
}

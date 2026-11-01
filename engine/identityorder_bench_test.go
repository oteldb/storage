package engine

import (
	"slices"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

func BenchmarkFlushedPartIndexSelection(b *testing.B) {
	const count = 65536
	files, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	counter := backendtest.NewByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "ordered-bench", MaxPartBytes: 4096 * partRowBytes,
		FlushMemoryBytes: 32 << 20})
	ids := make([]signal.SeriesID, 0, 512)
	for i := range count {
		s := orderBenchSeries(i)
		accepted, err := e.Append(s, 1, float64(i))
		if err != nil || !accepted {
			b.Fatalf("append: accepted=%v err=%v", accepted, err)
		}
		if i%128 == 0 {
			ids = append(ids, s.Hash())
		}
	}
	if err := e.Flush(b.Context()); err != nil {
		b.Fatal(err)
	}
	for _, part := range e.parts {
		if p := part.index.paged; p == nil || len(p.fences) == 0 {
			b.Fatal("fixture must exercise a paged index")
		} else {
			p.view.Store(nil)
		}
	}
	slices.SortFunc(ids, signal.SeriesID.Compare)
	counter.Reset()
	var readBytes int64
	b.ReportAllocs()
	for b.Loop() {
		plan := enginePlan{ids: ids, liveParts: e.parts}
		found := 0
		for _, part := range e.parts {
			ranges, err := plan.rangesFor(b.Context(), part)
			if err != nil {
				b.Fatal(err)
			}
			found += len(ranges)
		}
		if found != len(ids) {
			b.Fatalf("found %d of %d", found, len(ids))
		}
		readBytes += counter.Bytes()
		counter.Reset()
	}
	b.ReportMetric(float64(readBytes)/float64(b.N), "index-read-B/op")
}

func orderBenchSeries(i int) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
		Key: []byte("order"), Value: signal.IntValue(int64(i)),
	})}
}

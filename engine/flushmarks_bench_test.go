package engine

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkRawFlushFirstQuery(b *testing.B) {
	root := b.TempDir()
	files, err := file.New(filepath.Join(root, "parts"))
	if err != nil {
		b.Fatal(err)
	}
	counted := &marksCountingFile{File: files}
	const series, points = 32, 128
	ids, ts, values := make([]signal.SeriesID, series*points), make([]int64, series*points), make([]float64, series*points)
	for i := range ids {
		ids[i], ts[i], values[i] = orderSeries(i/points).Hash(), int64(100+i%points), float64(i)
	}
	var iteration, reads int64
	b.ReportAllocs()
	b.SetBytes(series * 4 * 16)
	for b.Loop() {
		b.StopTimer()
		iteration++
		name := strconv.FormatInt(iteration, 10)
		w, err := wal.Create(filepath.Join(root, "wal-"+name), 0)
		if err != nil {
			b.Fatal(err)
		}
		e := New(Config{Backend: counted, WAL: w, Prefix: "metrics-" + name, DecodeCacheBytes: 1 << 20})
		if _, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return orderSeries(i / points) }, AppendLimits{}); err != nil {
			b.Fatal(err)
		}
		if err := e.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
		counted.marksReads.Store(0)
		b.StartTimer()
		it, err := e.Fetch(b.Context(), fetch.Request{Start: 120, End: 123})
		if err != nil {
			b.Fatal(err)
		}
		rows, err := fetch.Drain(b.Context(), it)
		if err != nil || len(rows) != series {
			b.Fatalf("read: series=%d, err=%v", len(rows), err)
		}
		b.StopTimer()
		reads += counted.marksReads.Load()
		if err := e.CloseWAL(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(reads)/float64(b.N), "marks-reads/op")
}

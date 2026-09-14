package engine_test

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// BenchmarkAppendBatchWAL measures a durable metric append: admission, the WAL write of the batch,
// and applying it to the head — 1000 samples over 100 series per batch.
func BenchmarkAppendBatchWAL(b *testing.B) {
	const (
		seriesCount = 100
		perSeries   = 10
	)

	w, err := wal.Create(b.TempDir(), 0)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { _ = w.Close() })

	e := engine.New(engine.Config{WAL: w})

	all := make([]signal.Series, seriesCount)
	for i := range all {
		all[i] = mkSeries("__name__", "http_requests_total", "instance", fmt.Sprintf("host-%d", i))
	}

	n := seriesCount * perSeries
	ids := make([]signal.SeriesID, n)
	idents := make([]signal.Series, n)
	ts := make([]int64, n)
	values := make([]float64, n)

	for i := range n {
		s := all[i%seriesCount]
		ids[i], idents[i], values[i] = s.Hash(), s, float64(i)
	}

	mat := func(i int) signal.Series { return idents[i] }

	b.SetBytes(int64(n) * engine.SampleBytes)
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if i%256 == 0 {
			b.StopTimer()
			_ = e.Reset(b.Context())
			b.StartTimer()
		}

		for j := range ts {
			ts[j] = int64(i*n + j)
		}

		if _, err := e.AppendBatch(ids, ts, values, nil, mat, engine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}
}

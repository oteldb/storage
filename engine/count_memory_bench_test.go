package engine

import (
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkCountMemoryProof(b *testing.B) {
	const series = 65536
	col := make([]chunk.U128, series)
	for i := range col {
		col[i] = chunk.U128{Hi: uint64(i + 1)}
	}
	entries, n, total, err := parseSeriesIndex(encodeSeriesIndex(col), true)
	if err != nil {
		b.Fatal(err)
	}
	index := &pagedIndex{n: n, total: total}
	index.view.Store(&entries)
	plan := enginePlan{start: 0, end: 10}
	for i := range 1024 {
		plan.ids = append(plan.ids, signal.SeriesID(col[i*64]))
		plan.memActive = append(plan.memActive, true)
	}
	for range 64 {
		plan.liveParts = append(plan.liveParts, &part{minTime: 1, maxTime: 2, index: partIndex{paged: index}})
	}
	b.ReportAllocs()
	for b.Loop() {
		active, err := plan.activeFlags(b.Context(), plan.ids, nil)
		if err != nil || len(active) != 1024 {
			b.Fatalf("active=%d err=%v", len(active), err)
		}
	}
}

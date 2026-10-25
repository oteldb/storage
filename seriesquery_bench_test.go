package storage

import (
	"context"
	"testing"

	promstorage "github.com/prometheus/prometheus/storage"

	"github.com/oteldb/storage/query/promql"
)

func BenchmarkSeriesMetadata(b *testing.B) {
	ctx := context.Background()
	s, err := InMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(ctx); err != nil {
			b.Fatal(err)
		}
	})
	const seriesCount = 1000
	if _, err := s.WriteMetrics(ctx, benchBatch(seriesCount, 1000)); err != nil {
		b.Fatal(err)
	}
	if err := mustEngine(s.engineFor("default")).Flush(ctx); err != nil {
		b.Fatal(err)
	}
	queryable := promql.NewQueryable(s.Fetcher("default"), "default")
	hints := &promstorage.SelectHints{Func: "series"}
	b.ReportAllocs()
	for b.Loop() {
		q, err := queryable.Querier(0, 1<<40)
		if err != nil {
			b.Fatal(err)
		}
		ss := q.Select(ctx, true, hints)
		n := 0
		for ss.Next() {
			n++
		}
		if err := ss.Err(); err != nil {
			b.Fatal(err)
		}
		if n != seriesCount {
			b.Fatalf("got %d series, want %d", n, seriesCount)
		}
		if err := q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

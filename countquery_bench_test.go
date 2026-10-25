package storage

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/promql"
)

func BenchmarkCountNegativeMatcher(b *testing.B) {
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
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "route", "/route/0")}
	b.ReportAllocs()
	for b.Loop() {
		q, err := queryable.Querier(0, 1<<40)
		if err != nil {
			b.Fatal(err)
		}
		counter := q.(interface {
			CountSeries(ctx context.Context, startMs, endMs int64, matchers ...*labels.Matcher) (uint64, error)
		})
		n, err := counter.CountSeries(ctx, 0, 1<<40, ms...)
		if err != nil {
			b.Fatal(err)
		}
		if n != seriesCount-1 {
			b.Fatalf("got %d series, want %d", n, seriesCount-1)
		}
		if err := q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

package storage

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal/metric"
)

// BenchmarkAggregatePushdown compares computing a per-series aggregate the way an embedder does
// today — raw Fetch + fold every sample — against the storage-side pushdown (AggregateMetrics with
// the stats sidecar), which answers a range-covering aggregate from precomputed per-part stats
// without decoding the value column. The pushdown returns one number per series instead of decoding
// and merging every point.
func BenchmarkAggregatePushdown(b *testing.B) {
	ctx := context.Background()

	s, err := Open(ctx, Options{}, WithBackend(backend.Memory()), WithAggregateStats())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()

	if _, err := s.WriteMetrics(ctx, buildCorpus(corpusProfile{
		name: "c", series: 500, points: 100, interval: 15_000_000_000,
		kind: metric.KindGauge, pattern: patRandWalk,
	}, 1)); err != nil {
		b.Fatal(err)
	}
	eng := mustEngine(s.engineFor("default"))
	if err := eng.Flush(ctx); err != nil {
		b.Fatal(err)
	}
	if err := eng.Merge(ctx, 0); err != nil {
		b.Fatal(err)
	}

	req := fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("bench.metric")}}

	b.Run("fetch_fold", func(b *testing.B) {
		f := s.Fetcher("default")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			it, err := f.Fetch(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			var count int
			var sum float64
			for {
				batch, err := it.Next(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					b.Fatal(err)
				}
				for _, v := range batch.Values {
					sum += v
					count++
				}
			}
			_ = it.Close()
			_, _ = count, sum
		}
	})

	b.Run("pushdown", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := s.AggregateMetrics(ctx, "default", req); err != nil {
				b.Fatal(err)
			}
		}
	})
}

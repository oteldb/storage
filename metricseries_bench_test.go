package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/promql"
)

// opaqueFetcher hides every optional capability of the fetcher it wraps (it is not an
// [fetch.Unwraper]), so a query against it takes the fetch-and-drain fallback. It is how the
// enumeration benchmark measures the path the label endpoints used before [fetch.SeriesLister].
type opaqueFetcher struct{ inner fetch.Fetcher }

func (f opaqueFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return f.inner.Fetch(ctx, r)
}

// seriesOnlyFetcher exposes the identity-enumeration capability but hides the label-metadata one,
// so a benchmark can measure the enumeration path (resolve identities, project one label) against
// the index-driven path that never touches an identity.
type seriesOnlyFetcher struct{ inner fetch.SeriesLister }

func (f seriesOnlyFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return f.inner.(fetch.Fetcher).Fetch(ctx, r)
}

func (f seriesOnlyFetcher) Series(ctx context.Context, r fetch.Request) ([]signal.Series, error) {
	return f.inner.Series(ctx, r)
}

// BenchmarkLabelValues measures the Prometheus /api/v1/label/<name>/values path — the Grafana
// template-variable request — over a tenant with many series and a deep window, with no matcher
// (the shape Grafana issues, which matches every series). "enumerated" resolves identities through
// the [fetch.SeriesLister] capability; "drained" is the same query with the capability hidden, so
// it materializes every sample of every matching series to read the identity off each batch.
func BenchmarkLabelValues(b *testing.B) {
	ctx := context.Background()

	for _, shape := range []struct {
		name           string
		series, points int
	}{
		{"1000series_100points", 1000, 100},
		{"1000series_1000points", 1000, 1000},
		{"10000series_100points", 10000, 100},
	} {
		s, err := InMemory()
		if err != nil {
			b.Fatal(err)
		}

		if _, err := s.WriteMetrics(ctx, benchBatch(shape.series, shape.points)); err != nil {
			b.Fatal(err)
		}

		eng := mustEngine(s.engineFor("default"))
		if err := eng.Flush(ctx); err != nil {
			b.Fatal(err)
		}

		base := s.Fetcher("default")

		for _, mode := range []struct {
			name string
			f    fetch.Fetcher
		}{
			{"indexed", base},
			{"enumerated", seriesOnlyFetcher{inner: fetch.SeriesListerOf(base)}},
			{"drained", opaqueFetcher{inner: base}},
		} {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				q, err := promql.NewQueryable(mode.f, "default").Querier(0, 1<<40)
				if err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					vals, _, err := q.LabelValues(ctx, "route", nil)
					if err != nil {
						b.Fatal(err)
					}

					if len(vals) != shape.series {
						b.Fatalf("got %d values, want %d", len(vals), shape.series)
					}
				}

				b.StopTimer()

				if err := q.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}

		if err := s.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLabelValuesCardinality scales the query to a large tenant, over two label shapes: the
// dashboard-variable shape ("service.name" — a handful of values across every series, what Grafana
// resolves on a cold load) and the worst case ("route" — one distinct value per series, so the
// answer is as large as the series set). The index-driven path costs O(distinct values); the other
// two cost O(series) and O(samples) respectively.
func BenchmarkLabelValuesCardinality(b *testing.B) {
	ctx := context.Background()

	const points = 60 // a 15-minute window at a 15s scrape interval

	for _, seriesCount := range []int{100_000, 1_000_000} {
		s, err := InMemory()
		if err != nil {
			b.Fatal(err)
		}

		if _, err := s.WriteMetrics(ctx, benchBatch(seriesCount, points)); err != nil {
			b.Fatal(err)
		}

		eng := mustEngine(s.engineFor("default"))
		if err := eng.Flush(ctx); err != nil {
			b.Fatal(err)
		}

		base := s.Fetcher("default")

		// benchBatch stamps ts = 1e9 + p*15_000 (ns); start mid-way through the run so a part sits
		// at the window edge.
		const startMs = (1_000_000_000 + 30*15_000) / 1_000_000

		for _, label := range []struct {
			name string
			want int
		}{
			{"service.name", 1},
			{"route", seriesCount},
		} {
			for _, mode := range []struct {
				name string
				f    fetch.Fetcher
			}{
				{"indexed", base},
				{"enumerated", seriesOnlyFetcher{inner: fetch.SeriesListerOf(base)}},
				{"drained", opaqueFetcher{inner: base}},
			} {
				name := strconv.Itoa(seriesCount) + "series/" + label.name + "/" + mode.name

				b.Run(name, func(b *testing.B) {
					q, err := promql.NewQueryable(mode.f, "default").Querier(startMs, 1<<40)
					if err != nil {
						b.Fatal(err)
					}

					// One untimed call: the first read after ingest sorts the postings index in
					// place (a one-time cost of the write path, not of the query).
					if _, _, err := q.LabelValues(ctx, label.name, nil); err != nil {
						b.Fatal(err)
					}

					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						vals, _, err := q.LabelValues(ctx, label.name, nil)
						if err != nil {
							b.Fatal(err)
						}

						if len(vals) != label.want {
							b.Fatalf("got %d values, want %d", len(vals), label.want)
						}
					}

					b.StopTimer()

					if err := q.Close(); err != nil {
						b.Fatal(err)
					}
				})
			}
		}

		if err := s.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

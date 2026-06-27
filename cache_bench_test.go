package storage

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal/metric"
)

// slowBackend simulates a cold object store: every read pays a fixed latency. It reports
// non-ephemeral so the read cache engages (the in-memory backend is otherwise skipped).
type slowBackend struct {
	backend.Backend

	delay time.Duration
}

func (slowBackend) IsEphemeral() bool { return false }

func (s slowBackend) Read(ctx context.Context, key string) ([]byte, error) {
	time.Sleep(s.delay)

	return s.Backend.Read(ctx, key)
}

// BenchmarkReadCacheColdTier compares repeated fetches over a high-latency backend with the read
// cache off vs on. Without the cache each fetch re-reads the part's column objects over the (slow)
// backend; with it, the immutable objects are served from memory after the first fetch — the
// cold-tier tail-latency win. The absolute gain scales with backend latency × objects-read-per-
// fetch, so it is largest for real S3 latencies (tens of ms) and queries spanning many parts; this
// single-merged-part case at 1ms is a conservative floor.
func BenchmarkReadCacheColdTier(b *testing.B) {
	cases := []struct {
		name  string
		bytes int64
	}{
		{"cold", 0},
		{"cached", 64 << 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			slow := slowBackend{Backend: backend.Memory(), delay: time.Millisecond}

			s, err := Open(ctx, Options{}, WithBackend(slow), WithReadCache(tc.bytes))
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
			f := s.Fetcher("default")

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				it, err := f.Fetch(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				for {
					if _, err := it.Next(ctx); err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						b.Fatal(err)
					}
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

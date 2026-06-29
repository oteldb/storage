package engine_test

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// BenchmarkRecentQuery measures the latency of a recent-range query (the dashboard hot path) with and
// without the recent tier (issue #25 item 4). It models the steady workload's part count: many
// flushes each producing a part, so a recent query without the tier decodes every overlapping part;
// with the tier the query is served from RAM and acquires no part. (With a single part the tier
// loses — one decode-and-slice is cheaper than per-series bufBatch — so the bench uses many parts,
// the shape where the decode-avoidance pays off.)
func BenchmarkRecentQuery(b *testing.B) {
	const (
		series  = 500
		flushes = 50
	)

	mk := func() ([]signal.Series, []signal.SeriesID) {
		ser := make([]signal.Series, series)
		ids := make([]signal.SeriesID, series)
		for i := range ser {
			ser[i] = mkSeries("__name__", "cpu", "host", "h"+itoa(i))
			ids[i] = ser[i].Hash()
		}

		return ser, ids
	}

	for _, tiered := range []bool{false, true} {
		name := "no_tier"
		if tiered {
			name = "tier"
		}

		b.Run(name, func(b *testing.B) {
			ctx := context.Background()

			var cfg engine.Config
			if tiered {
				cfg = engine.Config{
					Backend:      backend.Memory(),
					Prefix:       "default/metrics",
					RecentWindow: int64(24 * 60 * 60 * 1e9), // 24h — covers all flushes
				}
			} else {
				cfg = engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"}
			}

			e := engine.New(cfg)

			ser, ids := mk()

			// `flushes` flushes, each appending `series` samples at an increasing ts — one part per
			// flush, so a recent query overlaps all of them.
			ts := make([]int64, series)
			vals := make([]float64, series)

			for f := range flushes {
				for i := range series {
					ts[i] = int64(f)*15 + int64(i)
					vals[i] = float64(f*series + i)
				}

				if _, err := e.AppendBatch(ids, ts, vals, nil, func(i int) signal.Series { return ser[i] }, engine.AppendLimits{}); err != nil {
					b.Fatal(err)
				}

				if err := e.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}

			req := fetch.Request{Start: 0, End: 1 << 62}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				it, err := e.Fetch(ctx, req)
				if err != nil {
					b.Fatal(err)
				}

				batches, err := fetch.Drain(ctx, it)
				if err != nil {
					b.Fatal(err)
				}

				if len(batches) != series {
					b.Fatalf("got %d batches, want %d", len(batches), series)
				}
			}
		})
	}
}

func itoa(i int) string {
	// small, allocation-light int→string for the bench corpus
	if i == 0 {
		return "0"
	}

	var buf [12]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}

	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}

	if neg {
		pos--
		buf[pos] = '-'
	}

	return string(buf[pos:])
}

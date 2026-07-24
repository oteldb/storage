package recordengine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkLimitScanParts drives a top-N scan over many fully time-overlapping parts: no part can be
// pruned by the watermark, so every one is read and the watermark is re-evaluated after each. It is
// the worst case for the top-N bookkeeping — the accumulated row count grows with every part — and
// therefore the case that shows whether the watermark is maintained incrementally or rebuilt.
//
//	go test -run=^$ -bench=^BenchmarkLimitScanParts -benchmem ./recordengine
func BenchmarkLimitScanParts(b *testing.B) {
	for _, parts := range []int{8, 32, 64} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			const (
				streams       = 4
				rowsPerStream = 2000
				limit         = 1000
			)

			ctx := context.Background()
			e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})

			for p := range parts {
				for s := range streams {
					recs := make([]rrec, rowsPerStream)
					for i := range recs {
						// Every part spans the same window, so none is beyond the watermark.
						recs[i] = rrec{
							ts:   int64(i*10 + s),
							sev:  int64(i%24 + 1),
							body: fmt.Sprintf("p%d-s%d-r%d", p, s, i),
						}
					}

					if _, err := e.AppendBatch(mkBatch(fmt.Sprintf("svc-%d", s), recs...), recordengine.AppendLimits{}); err != nil {
						b.Fatal(err)
					}
				}

				require.NoError(b, e.Flush(ctx))
			}

			r := fetch.Request{
				Signal: signal.Log, Start: 0, End: 1 << 62,
				Projection: []string{"body"},
				Limit:      limit, Reverse: true,
			}

			b.ReportAllocs()
			b.ResetTimer()

			var got int
			for range b.N {
				it, err := e.Fetch(ctx, r)
				if err != nil {
					b.Fatal(err)
				}

				bs, err := fetch.Drain(ctx, it)
				if err != nil {
					b.Fatal(err)
				}

				got = 0
				for _, bt := range bs {
					got += len(bt.Timestamps)
				}
			}

			require.GreaterOrEqual(b, got, limit)
		})
	}
}

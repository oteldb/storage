package recordengine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkFetchManyParts reads one stream across many zstd-compressed merged parts, one part per
// day: a query whose cost is dominated by opening and decoding each part once, not by any one part.
func BenchmarkFetchManyParts(b *testing.B) {
	const (
		perDay = 64
		day    = int64(24 * time.Hour)
	)

	ctx := context.Background()

	for _, days := range []int{16, 256} {
		e := recordengine.New(recordengine.Config{
			Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs",
			MergeCompression: compress.AlgorithmZSTD, MergeMemoryBytes: -1,
		})

		body := strings.Repeat("x", 64)
		recs := make([]rrec, 0, days*perDay)

		for d := range days {
			for i := range perDay {
				recs = append(recs, rrec{ts: int64(d)*day + int64(i)*int64(time.Minute), sev: int64(i), body: body})
			}
		}

		for i := 0; i < len(recs); i += 4096 {
			if _, err := e.AppendBatch(mkBatch("api", recs[i:min(i+4096, len(recs))]...), recordengine.AppendLimits{}); err != nil {
				b.Fatal(err)
			}
		}

		if err := e.Flush(ctx); err != nil {
			b.Fatal(err)
		}

		if err := e.MergeWith(ctx, recordengine.MergeOptions{Force: true}); err != nil {
			b.Fatal(err)
		}

		if got := len(e.Parts()); got < days {
			b.Fatalf("%d parts, want one per day (%d)", got, days)
		}

		req := fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{svcMatcher("api")}}

		b.Run(fmt.Sprintf("parts=%d", days), func(b *testing.B) {
			b.SetBytes(int64(len(recs)) * (8 + 8 + int64(len(body))))
			b.ReportAllocs()

			for b.Loop() {
				it, err := e.Fetch(ctx, req)
				if err != nil {
					b.Fatal(err)
				}

				n := 0

				batches, err := fetch.Drain(ctx, it)
				if err != nil {
					b.Fatal(err)
				}

				for _, bt := range batches {
					n += len(bt.Timestamps)
				}

				if n != len(recs) {
					b.Fatalf("fetched %d rows, want %d", n, len(recs))
				}
			}
		})
	}
}

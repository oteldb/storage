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

// BenchmarkLazyWindowScan drives the two-phase filtered scan (fetchlazy.go) over one flushed part
// holding a wide per-stream range, with a condition that matches every row. It contrasts a full-window
// fetch (whole range) against a narrow window (~1% of the range): tsWindow binary-searches each
// stream's ts-sorted range to the in-window sub-range, so the narrow case evaluates the condition on
// only the matching rows instead of the whole part.
//
//	go test -run=^$ -bench=^BenchmarkLazyWindowScan -benchmem ./recordengine
func BenchmarkLazyWindowScan(b *testing.B) {
	const rows = 50000

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})

	recs := make([]rrec, rows)
	for i := range recs {
		recs[i] = rrec{
			ts:   int64(i + 1),
			sev:  int64(i%24 + 1),
			body: fmt.Sprintf("GET /api/v1/resource/%d status=200", i%256),
			id:   fmt.Sprintf("%016x", i),
		}
	}

	if _, err := e.AppendBatch(mkBatch(scanStream, recs...), recordengine.AppendLimits{}); err != nil {
		b.Fatal(err)
	}
	require.NoError(b, e.Flush(ctx))

	// A condition on an int column that matches every row, so the scan cost is governed by how many
	// rows fall in the window, not by selectivity.
	matchAll := fetch.Condition{Column: "sev", Match: func(v signal.Value) bool { return v.Int() >= 1 }}

	run := func(b *testing.B, start, end int64, want int) {
		b.Helper()

		r := fetch.Request{
			Signal: signal.Log, Start: start, End: end,
			Matchers:      []fetch.Matcher{svcMatcher(scanStream)},
			Conditions:    []fetch.Condition{matchAll},
			AllConditions: true,
			Projection:    []string{"body"},
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

		require.Equal(b, want, got)
	}

	b.Run("Full", func(b *testing.B) { run(b, 0, 1<<60, rows) })
	b.Run("Narrow", func(b *testing.B) { run(b, rows/2, rows/2+rows/100, rows/100+1) })
}

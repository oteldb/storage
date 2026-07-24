package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// BenchmarkCondEval drives the filtered scan (fetchlazy.go/fetcheval.go) over one flushed part with
// predicates that are expensive per call — a substring scan over a dictionary-coded body, and an
// attribute lookup that parses the serialized attrs blob. Both columns are dictionary-encoded, so
// the per-entry memo is what decides whether the predicate runs once per row or once per distinct
// value; Multi additionally exercises the cheap-first ordering across four conditions.
//
//	go test -run=^$ -bench=^BenchmarkCondEval -benchmem ./recordengine
func BenchmarkCondEval(b *testing.B) {
	const (
		rows     = 50000
		distinct = 256
	)

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})

	recs := make([]rrec, rows)
	for i := range recs {
		recs[i] = rrec{
			ts:   int64(i + 1),
			sev:  int64(i%24 + 1),
			body: fmt.Sprintf("GET /api/v1/resource/%d status=200", i%distinct),
			id:   fmt.Sprintf("%016x", i%distinct),
			attr: [2]string{"user", fmt.Sprintf("user-%d", i%distinct)},
		}
	}

	if _, err := e.AppendBatch(mkBatch(scanStream, recs...), recordengine.AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	require.NoError(b, e.Flush(ctx))

	contains := func(column, sub string) fetch.Condition {
		want := []byte(sub)

		return fetch.Condition{
			Column: column,
			Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		}
	}

	run := func(b *testing.B, want int, conds ...fetch.Condition) {
		b.Helper()

		r := fetch.Request{
			Signal: signal.Log, Start: 0, End: 1 << 60,
			Matchers:      []fetch.Matcher{svcMatcher(scanStream)},
			Conditions:    conds,
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

	// One attribute value out of `distinct` carries user-7.
	user7 := fetch.Condition{Column: "user", Match: eqPredicate("user-7")}

	attrWant := 0
	for i := range recs {
		if i%distinct == 7 {
			attrWant++
		}
	}

	// status=200 is on every row: the scan gathers everything, so the cost is the predicate itself.
	b.Run("Body", func(b *testing.B) { run(b, rows, contains("body", "status=200")) })
	// Highly selective, but on an attribute: without the memo every row re-parses the attrs blob.
	b.Run("Attr", func(b *testing.B) { run(b, attrWant, user7) })
	b.Run("Multi", func(b *testing.B) {
		run(b, attrWant, user7, contains("body", "status=200"), sevAtLeast(1))
	})
}

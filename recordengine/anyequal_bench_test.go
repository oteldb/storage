package recordengine_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// The TraceQL "resolve ids, then fetch those traces" shape: 500 traces of 8 spans each. The set of
// candidate ids is what a search returns, so the interesting axis is N — one part measures the scan
// side, several parts measure the prune side.
const (
	benchTraces    = 500
	benchSpans     = 8
	benchPartCount = 16
)

// benchTraceID is the 16-byte id of trace n, the width the raw id column carries.
func benchTraceID(n int) []byte {
	id := make([]byte, 16)
	binary.BigEndian.PutUint64(id[8:], uint64(n))

	return id
}

// benchSetEngine writes benchTraces traces of benchSpans spans across parts parts, so a set of ids
// drawn from one part leaves the rest prunable.
func benchSetEngine(b *testing.B, parts int) *recordengine.Engine {
	b.Helper()

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: rawIDSchema, Backend: backend.Memory(), Prefix: "t/rawid"})

	perPart := benchTraces / parts

	for p := range parts {
		recs := make([]rawIDRec, 0, perPart*benchSpans)

		for t := p * perPart; t < (p+1)*perPart; t++ {
			for s := range benchSpans {
				recs = append(recs, rawIDRec{
					ts:   int64(t*benchSpans + s + 1),
					body: fmt.Sprintf("span %d of trace %d", s, t),
					id:   benchTraceID(t),
				})
			}
		}

		if _, err := e.AppendBatch(mkRawIDBatch("api", recs...), recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}

		require.NoError(b, e.Flush(ctx))
	}

	require.Equal(b, parts, e.PartCount())

	return e
}

// benchIDs returns n ids drawn from the front of the corpus, so with several parts they all live in
// the first part and the rest are prunable.
func benchIDs(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = benchTraceID(i)
	}

	return out
}

// closureSetCondition is the status quo: a set expressed as a bare Match closure over a map, which
// is what an embedder is forced into today.
func closureSetCondition(ids [][]byte) fetch.Condition {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[string(id)] = struct{}{}
	}

	return fetch.Condition{
		Column: "trace_id",
		Match: func(v signal.Value) bool {
			_, ok := set[string(v.Str())]

			return ok
		},
	}
}

// BenchmarkAnyEqualFetch contrasts the three ways the same predicate can be expressed: the existing
// single-value Equal fast path, the new set hint, and the bare Match closure the set degrades to
// today.
//
//	go test -run=^$ -bench=^BenchmarkAnyEqualFetch -benchmem ./recordengine
func BenchmarkAnyEqualFetch(b *testing.B) {
	for _, parts := range []int{1, benchPartCount} {
		e := benchSetEngine(b, parts)

		run := func(b *testing.B, c fetch.Condition, wantRows int) {
			b.Helper()

			ctx := context.Background()
			r := fetch.Request{
				Start: 0, End: 1 << 60,
				Conditions: []fetch.Condition{c}, AllConditions: true,
				Projection: []string{"body"},
			}

			b.ReportAllocs()
			b.ResetTimer()

			got := 0

			for range b.N {
				it, err := e.Fetch(ctx, r)
				if err != nil {
					b.Fatal(err)
				}

				batches, err := fetch.Drain(ctx, it)
				if err != nil {
					b.Fatal(err)
				}

				got = 0
				for _, bt := range batches {
					got += len(bt.Timestamps)
				}
			}

			require.Equal(b, wantRows, got)
		}

		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			b.Run("Equal/N=1", func(b *testing.B) {
				run(b, traceIDEquals(benchTraceID(0)), benchSpans)
			})

			for _, n := range []int{1, 8, 64} {
				ids := benchIDs(n)

				b.Run(fmt.Sprintf("AnyEqual/N=%d", n), func(b *testing.B) {
					run(b, setCondition("trace_id", ids, nil), n*benchSpans)
				})

				b.Run(fmt.Sprintf("Closure/N=%d", n), func(b *testing.B) {
					run(b, closureSetCondition(ids), n*benchSpans)
				})
			}
		})
	}
}

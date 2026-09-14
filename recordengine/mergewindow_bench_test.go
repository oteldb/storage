package recordengine

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

// BenchmarkMergeRetentionWindow times what a retention merge spends per source part before any row is
// written: decoding the part and gathering one stream's rows at or after the retention horizon, which
// here drops the older half.
func BenchmarkMergeRetentionWindow(b *testing.B) {
	ctx := context.Background()
	e := New(Config{Schema: headTestSchema, Backend: backend.Memory(), Prefix: "t/w"})

	const n = 200_000

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	ts := make([]int64, n)
	sev := make([]int64, n)
	bodies := make([][]byte, n)

	for i := range n {
		ts[i], sev[i], bodies[i] = int64(i), int64(i%5), []byte("GET /api/v1/thing status=200 done")
	}

	if _, err := e.AppendBatch(&Batch{
		Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{bodies},
	}, AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	p := e.parts[0]
	rng, ok := p.lookup(series.Hash())
	if !ok {
		b.Fatal("stream not in part")
	}

	acc := newRecordCols(headTestSchema, 0, fullSel(headTestSchema))

	b.SetBytes(int64(n) * 16)
	b.ReportAllocs()

	for b.Loop() {
		d, err := p.readForMerge(ctx)
		if err != nil {
			b.Fatal(err)
		}

		acc.prepare(headTestSchema, 0, fullSel(headTestSchema))
		appendMergeWindow(acc, d, 0, rng, n/2, maxInt64)
	}
}

package recordengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// BenchmarkGranulePrunedFetch measures a windowed read of one part end to end: what decoding costs
// when the window selects a fraction of the granules against the whole part. The sub-benchmark names
// carry the selection, so a change that quietly stops pruning shows up as 25of25 rather than as a
// number nobody compares.
func BenchmarkGranulePrunedFetch(b *testing.B) {
	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	)

	const rows = 200_000

	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/recs"})
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}
	id := series.Hash()

	ts := make([]int64, rows)
	sev := make([]int64, rows)
	bodies := make([][]byte, rows)
	for i := range rows {
		ts[i] = int64(i)
		sev[i] = int64(i % 9)
		bodies[i] = fmt.Appendf(nil, "GET /api/v1/resource/%d status=200 done", i%64)
	}
	if _, err := e.AppendBatch(&Batch{
		Stream: id, Identity: func() signal.Series { return series },
		Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{bodies},
	}, AppendLimits{}); err != nil {
		b.Fatal(err)
	}
	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	p := e.parts[0]
	ids := []signal.SeriesID{id}
	sel := fullSel(schema)
	total := len(p.granuleTimes(ctx))
	b.Logf("part has %d granules over %d rows", total, rows)

	for _, w := range []struct {
		name   string
		lo, hi int64
	}{
		{"whole", minInt64, maxInt64},
		{"half", 0, rows / 2},
		{"tenth", 0, rows / 10},
		{"hundredth", 0, rows / 100},
	} {
		blocks := p.windowGranules(ctx, ids, w.lo, w.hi)
		b.Run(fmt.Sprintf("%s_%dof%d", w.name, len(blocks), total), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := p.readCols(ctx, sel, nil, blocks); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

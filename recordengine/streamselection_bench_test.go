package recordengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func streamSelectionBenchEngine(b *testing.B) (*Engine, []signal.SeriesID) {
	b.Helper()
	e := New(Config{Schema: NewSchema(Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64}), Backend: backend.Memory(), Prefix: "bench/selection"})
	ids := make([]signal.SeriesID, 512)
	for part := range 12 {
		for s := range ids {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("stream"), Value: signal.IntValue(int64(s))})}}
			ids[s] = series.Hash()
			ts := int64(part * 10)
			if _, err := e.AppendBatch(&Batch{Stream: ids[s], Identity: func() signal.Series { return series }, Ts: []int64{ts, ts + 1}, Ints: [][]int64{{0, 1}}}, AppendLimits{}); err != nil {
				b.Fatal(err)
			}
		}
		if err := e.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	return e, sortedIDsOf(ids)
}

func BenchmarkFetchStreamSelection(b *testing.B) {
	e, ids := streamSelectionBenchEngine(b)
	for _, budgeted := range []bool{false, true} {
		for _, filtered := range []bool{false, true} {
			b.Run(fmt.Sprintf("budget=%t/filtered=%t", budgeted, filtered), func(b *testing.B) {
				ctx := context.Background()
				if budgeted {
					ctx = readbudget.With(ctx, readbudget.New(64<<20))
				}
				req := fetch.Request{Start: 0, End: maxInt64, AllConditions: filtered}
				if filtered {
					req.Conditions = []fetch.Condition{{Column: "severity", Match: func(v signal.Value) bool { return v.Int() == 1 }}}
				}
				want := len(ids) * 12 * 2
				if filtered {
					want /= 2
				}
				b.ReportAllocs()
				for b.Loop() {
					it, err := e.Fetch(ctx, req)
					if err != nil {
						b.Fatal(err)
					}
					batches, err := fetch.Drain(ctx, it)
					if err != nil {
						b.Fatal(err)
					}
					var n int
					for _, batch := range batches {
						n += len(batch.Timestamps)
					}
					if n != want {
						b.Fatalf("got %d rows, want %d", n, want)
					}
				}
			})
		}
	}
}

func BenchmarkPlanStreamSelection(b *testing.B) {
	e, ids := streamSelectionBenchEngine(b)
	for _, budgeted := range []bool{false, true} {
		b.Run(fmt.Sprintf("budget=%t", budgeted), func(b *testing.B) {
			ctx := context.Background()
			if budgeted {
				ctx = readbudget.With(ctx, readbudget.New(64<<20))
			}
			req := fetch.Request{Start: 0, End: maxInt64}
			b.ReportAllocs()
			for b.Loop() {
				plan, err := e.planFetch(ctx, ids, req)
				if err != nil {
					b.Fatal(err)
				}
				plan.releaseParts()
				plan.releaseBudget()
			}
		})
	}
}

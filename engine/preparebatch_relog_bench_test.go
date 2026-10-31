package engine

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkPreparedWALIdentityRelog stages registered identities without active sample buffers,
// as after a flush. The registry and per-request scratch are warm; each batch relogs identities.
func BenchmarkPreparedWALIdentityRelog(b *testing.B) {
	for _, points := range []int{512, 4096} {
		for _, attributes := range []int{4, 12} {
			b.Run(fmt.Sprintf("points=%d/attributes=%d", points, attributes), func(b *testing.B) {
				h := newHead()
				ids := make([]signal.SeriesID, points)
				for i := range ids {
					attrs := make([]signal.KeyValue, attributes)
					for j := range attrs {
						attrs[j] = signal.KeyValue{Key: fmt.Appendf(nil, "key-%02d", j), Value: signal.IntValue(int64(j))}
					}
					attrs[0].Value = signal.IntValue(int64(i))
					s := signal.Series{Attributes: signal.NewAttributes(attrs...)}
					h.registerSeries(s)
					ids[i] = s.Hash()
				}
				view := h.series.View()
				pool := newBatchPreparations()
				materialize := func() signal.Series { panic("registered identity materialized") }
				run := func() {
					p := pool.acquire()
					p.prepare(ids)
					for i, id := range ids {
						p.current = p.positions[i]
						if p.admit(h, id, 1, 1, 1, 0, AppendLimits{}, materialize) != admitted {
							b.Fatal("registered sample rejected")
						}
					}
					if err := p.batch.encode(view, false); err != nil {
						b.Fatal(err)
					}
					pool.release(p)
				}
				run()
				b.SetBytes(int64(points) * SampleBytes)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					run()
				}
			})
		}
	}
}

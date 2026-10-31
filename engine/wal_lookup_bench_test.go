package engine

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func BenchmarkAppendWALRegistry(b *testing.B) {
	for _, cardinality := range []int{1 << 16, 1 << 20} {
		for _, ooo := range []int64{0, 1000} {
			b.Run(fmt.Sprintf("series=%d/ooo=%d", cardinality, ooo), func(b *testing.B) {
				const points = 4096
				w, err := wal.Create(b.TempDir(), 1<<30)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = w.Close() })
				e := New(Config{WAL: w, OOOWindow: ooo})
				ids := make([]signal.SeriesID, cardinality)
				ts, vals := make([]int64, points), make([]float64, points)
				for i := range ts {
					ts[i], vals[i] = 1, 1
				}
				identity := func(i int) signal.Series {
					return signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
						Key: []byte("instance"), Value: signal.IntValue(int64(i)),
					})}
				}
				for i := range ids {
					ids[i] = identity(i).Hash()
				}
				for offset := 0; offset < cardinality; offset += points {
					if _, err := e.AppendBatch(ids[offset:offset+points], ts, vals, nil,
						func(i int) signal.Series { return identity(offset + i) }, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
				}
				var cursor atomic.Uint64
				materialize := func(int) signal.Series { panic("warm identity was materialized") }
				b.SetBytes(points * SampleBytes)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						offset := int(cursor.Add(points)-points) % cardinality
						if _, err := e.AppendBatch(ids[offset:offset+points], ts, vals, nil, materialize, AppendLimits{}); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

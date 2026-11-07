package engine

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkHeadStateResidentBytes(b *testing.B) {
	const count = 1 << 20
	for _, percent := range []int{1, 100} {
		b.Run("active="+strconv.Itoa(percent), func(b *testing.B) {
			var retained, accounted int64
			for b.Loop() {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				h := newHead()
				for i := range count {
					h.registerSeries(signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
						Key: []byte("instance"), Value: signal.IntValue(int64(i)),
					})})
				}
				active := count * percent / 100
				for i := range active {
					id := h.series.ID(uint32(i * count / active))
					h.bufFor(id).appendSample(1, 1, 1)
					h.noteTS(id, 1)
					h.grow(SampleBytes)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				retained += int64(after.HeapAlloc) - int64(before.HeapAlloc)
				accounted += h.identityBytes()
				runtime.KeepAlive(h)
			}
			b.ReportMetric(float64(retained)/float64(b.N), "retained-B/head")
			b.ReportMetric(float64(accounted)/float64(b.N), "identity-B/head")
		})
	}
}

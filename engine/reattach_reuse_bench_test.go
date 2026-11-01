package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
)

func BenchmarkHeadReattach(b *testing.B) {
	for _, frozen := range []int{1 << 16, 1 << 20} {
		for _, active := range []int{0, frozen / 4, frozen} {
			b.Run(fmt.Sprintf("frozen=%d/active=%d", frozen, active), func(b *testing.B) {
				buffers := make([]sampleBuf, frozen)
				oldTS := make([]int64, 2*frozen)
				oldValues := make([]float64, 2*frozen)
				detached := make(map[signal.SeriesID]*sampleBuf, frozen)
				for i := range buffers {
					oldTS[2*i], oldValues[2*i] = 1, 1
					buffers[i] = sampleBuf{ts: oldTS[2*i : 2*i+1 : 2*i+2], values: oldValues[2*i : 2*i+1 : 2*i+2]}
					detached[signal.SeriesID{Lo: uint64(i)}] = &buffers[i]
				}
				live := sampleBuf{ts: []int64{2}, values: []float64{2}}
				since := time.Unix(1, 0)
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					current := make(map[signal.SeriesID]*sampleBuf, active)
					for i := range active {
						buffers[i].ts, buffers[i].values = buffers[i].ts[:1], buffers[i].values[:1]
						current[signal.SeriesID{Lo: uint64(i)}] = &live
					}
					h := head{samples: current, bytes: int64(active) * SampleBytes, detachedBytes: int64(frozen) * SampleBytes}
					b.StartTimer()
					h.reattach(detached, int64(frozen)*SampleBytes, since)
					if len(h.samples) != frozen {
						b.Fatalf("head has %d series, want %d", len(h.samples), frozen)
					}
				}
			})
		}
	}
}

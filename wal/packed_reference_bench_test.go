package wal

import (
	"bytes"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkReferenceFrames(b *testing.B) {
	ids := make([]signal.SeriesID, MaxPackedReferences)
	for i := range ids {
		ids[i] = signal.HashBytes([]byte{byte(i >> 8), byte(i)})
	}
	for _, fixture := range []struct {
		name string
		step int64
	}{
		{"same_timestamp", 0},
		{"millisecond_jitter", 1_000_000},
	} {
		sample := func(i int) (int64, float64) {
			return packedTestTimestamp + fixture.step*int64(i%8), float64(i) / 7
		}
		for _, packed := range []bool{false, true} {
			name := fixture.name + "/legacy"
			if packed {
				name = fixture.name + "/packed"
			}
			b.Run(name, func(b *testing.B) {
				var dst bytes.Buffer
				w := NewWriter(&dst)
				write := func(b *testing.B) {
					b.Helper()
					dst.Reset()
					if packed {
						written, err := w.WritePackedReferences(ids, sample)
						if !written || err != nil {
							b.Fatalf("write failed: %t %v", written, err)
						}
						return
					}
					for i, id := range ids {
						ts, value := sample(i)
						if err := w.WriteReferencedSamples(id, []int64{ts}, []float64{value}, nil, false); err != nil {
							b.Fatal(err)
						}
					}
				}
				write(b)
				const logicalBytes = MaxPackedReferences * (seriesIDLen + 8 + 8)
				b.Run("encode", func(b *testing.B) {
					b.SetBytes(logicalBytes)
					b.ReportAllocs()
					for b.Loop() {
						write(b)
					}
					b.ReportMetric(float64(dst.Len())/MaxPackedReferences, "WAL-B/point")
				})
				b.Run("replay", func(b *testing.B) {
					b.SetBytes(logicalBytes)
					b.ReportAllocs()
					h := Handlers{
						OnDescriptorReference: func(signal.SeriesID) error { return nil },
						OnSamples:             func(signal.SeriesID, []int64, []float64) error { return nil },
						OnPackedReference:     func(signal.SeriesID, int64, float64) error { return nil },
					}
					for b.Loop() {
						if err := Replay(dst.Bytes(), h); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

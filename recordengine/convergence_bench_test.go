package recordengine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkResidentLimitedMergeRetries(b *testing.B) {
	b.ReportAllocs()
	var finalParts int
	for b.Loop() {
		b.StopTimer()
		e := wideDictEngine(b, backend.Memory(), Config{
			MaxPartBytes: 64 << 20, MergeCeilingBytes: 1 << 30, MergeMemoryBytes: 32 << 20,
			MergeCompression: compress.AlgorithmZSTD, MergeCompressionLevel: compress.LevelFast,
		}, 3, 4, 128<<10, 1)
		var logical int64
		for _, p := range e.parts {
			logical += p.decodedBytes()
		}
		b.SetBytes(logical)
		b.StartTimer()
		for range 12 {
			if err := e.MergeWith(b.Context(), MergeOptions{Force: true}); err != nil {
				b.Fatal(err)
			}
		}
		finalParts = e.PartCount()
		b.StopTimer()
		if err := e.Close(b.Context()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(finalParts), "parts")
}

func BenchmarkRecordMergeSelection(b *testing.B) {
	for _, count := range []int{64, 1024} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			e := New(Config{Schema: headTestSchema, MergeMemoryBytes: -1})
			parts := make([]*part, count)
			for i := range parts {
				parts[i] = partOfSize(i, 1<<20)
				parts[i].minTime, parts[i].maxTime = 1, 2
			}
			b.ReportAllocs()
			for b.Loop() {
				if len(e.selectMergeParts(parts, 0, 1<<30, false)) != maxTierParts {
					b.Fatal("unexpected selection")
				}
			}
		})
	}
}

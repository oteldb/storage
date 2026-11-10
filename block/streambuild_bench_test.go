package block

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
)

type discardBuildBackend struct{ backend.Backend }

func (discardBuildBackend) Write(context.Context, string, []byte) error { return nil }

// BenchmarkStreamBuildMetadata isolates build/write metadata after constant columns are prepared.
// Use -benchtime=100x so per-iteration preparation does not dominate wall-clock calibration.
func BenchmarkStreamBuildMetadata(b *testing.B) {
	be := discardBuildBackend{Backend: backend.Memory()}
	for _, reuse := range []bool{false, true} {
		b.Run("reuse="+strconv.FormatBool(reuse), func(b *testing.B) {
			var scratch StreamBuildScratch
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				w := NewStreamWriter(WithGranuleSize(1))
				for i := range 4 {
					if err := w.AddColumn(Column{Name: strconv.Itoa(i), Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}); err != nil {
						b.Fatal(err)
					}
					if err := w.AppendInt64(i, []int64{7}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				var err error
				if reuse {
					err = WriteStreamPartWithScratch(b.Context(), be, "part", w, &scratch)
				} else {
					err = WriteStreamPart(b.Context(), be, "part", w)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

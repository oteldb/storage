package block

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

type discardFrameWriter struct{}

func (discardFrameWriter) Write(p []byte) (int, error)  { return len(p), nil }
func (discardFrameWriter) Commit(context.Context) error { return nil }
func (discardFrameWriter) Abort()                       {}

func BenchmarkStreamedFrameAllocation(b *testing.B) {
	rnd := rand.New(rand.NewPCG(97, 101))
	input := make([]byte, 64<<10)
	for i := range input {
		input[i] = byte(rnd.Uint64() % 64)
	}

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		b.Run(alg.String(), func(b *testing.B) {
			comp := compress.NewFrameCompressor(alg, compress.LevelDefault, len(input))
			b.ReportAllocs()
			b.SetBytes(int64(32 * len(input)))
			for b.Loop() {
				a := newBlockAccum(comp, len(input))
				if err := a.attach(func() (backend.ObjectWriter, error) { return discardFrameWriter{}, nil }); err != nil {
					b.Fatal(err)
				}
				for range 32 {
					if err := a.addGranule(func(dst []byte) ([]byte, error) { return append(dst, input...), nil }); err != nil {
						b.Fatal(err)
					}
				}
				if _, _, err := a.finish(context.Background(), 1, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

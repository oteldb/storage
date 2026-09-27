//go:build !gozstd && !race

package compress

import (
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/heaptest"
)

// TestEncodeWorkspaceBoundsHeap: an encoder fed twice its window holds no more than
// [Compressor.EncodeWorkspace] says, and not so much less that the figure is a guess.
//
//nolint:paralleltest // samples the process-wide heap
func TestEncodeWorkspaceBoundsHeap(t *testing.T) {
	src := make([]byte, encoderWindowBytes)

	r := rand.New(rand.NewPCG(1, 2))
	for i := range src {
		src[i] = byte('a' + r.IntN(8))
	}

	for _, window := range []int{128 << 10, 1 << 20, encoderWindowBytes} {
		for _, level := range []Level{LevelFast, LevelDefault, LevelBest} {
			c := NewFrameCompressor(AlgorithmZSTD, level, window)

			// A second collection empties the pools of the compressors measured before.
			runtime.GC()

			base := heaptest.Live()
			enc := c.newEncoder().(zstdEncoder)
			out := enc.encodeAll(nil, src[:min(len(src), 2*window)])
			held := int64(heaptest.Live()) - int64(base) - int64(cap(out))

			runtime.KeepAlive(enc)

			t.Logf("window %d KiB, level %d: encoder holds %.2f MiB, figure %.2f MiB",
				window>>10, level, float64(held)/(1<<20), float64(c.EncodeWorkspace())/(1<<20))
			assert.LessOrEqual(t, held, c.EncodeWorkspace(), "window %d, level %d", window, level)
			assert.Greater(t, held, c.EncodeWorkspace()/3, "window %d, level %d", window, level)
		}
	}
}

// TestFrameCompressorMatchesWithinWindow: an input that fits a frame compressor's window compresses
// to the bytes the default window gives, so a smaller window changes no part a merge writes from
// frames that fit it; a larger input still round-trips.
func TestFrameCompressorMatchesWithinWindow(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(3, 4))

	for _, level := range []Level{LevelFast, LevelDefault, LevelBest} {
		full := NewCompressor(AlgorithmZSTD, level)
		framed := NewFrameCompressor(AlgorithmZSTD, level, 128<<10)

		for _, n := range []int{0, 1, 4 << 10, 64 << 10, 128 << 10, 1 << 20} {
			src := make([]byte, n)
			for i := range src {
				src[i] = byte('a' + r.IntN(12))
			}

			got := framed.Compress(nil, src)
			if n <= 128<<10 {
				assert.Equal(t, full.Compress(nil, src), got, "level %d, %d bytes", level, n)
			}

			back, err := full.Decompress(nil, got)
			require.NoError(t, err)
			assert.Equal(t, string(src), string(back), "level %d, %d bytes", level, n)
		}
	}
}

func TestFrameWindow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want int }{
		{0, minEncoderWindow},
		{minEncoderWindow, minEncoderWindow},
		{minEncoderWindow + 1, 2 * minEncoderWindow},
		{64 << 10, 64 << 10},
		{100 << 10, 128 << 10},
		{1 << 30, encoderWindowBytes},
	} {
		assert.Equal(t, tc.want, frameWindow(tc.in), "%d", tc.in)
	}
}

//go:build !gozstd && !race

package compress

import (
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/heaptest"
)

// TestEncodeWorkspaceBoundsHeap: an encoder whose window has filled holds no more than
// [Compressor.EncodeWorkspace] says, and not so much less that the figure is a guess.
//
//nolint:paralleltest // samples the process-wide heap
func TestEncodeWorkspaceBoundsHeap(t *testing.T) {
	src := make([]byte, encoderWindowBytes)

	r := rand.New(rand.NewPCG(1, 2))
	for i := range src {
		src[i] = byte('a' + r.IntN(8))
	}

	for _, level := range []Level{LevelFast, LevelDefault, LevelBest} {
		c := NewCompressor(AlgorithmZSTD, level)

		base := heaptest.Live()
		out := c.Compress(nil, src)
		held := int64(heaptest.Live()) - int64(base) - int64(cap(out))

		runtime.KeepAlive(c)

		t.Logf("level %d: encoder holds %.1f MiB, figure %.1f MiB", level, float64(held)/(1<<20), float64(c.EncodeWorkspace())/(1<<20))
		assert.LessOrEqual(t, held, c.EncodeWorkspace(), "level %d", level)
		assert.Greater(t, held, c.EncodeWorkspace()/2, "level %d", level)
	}
}

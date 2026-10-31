package block

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

//nolint:paralleltest // Mutates the process-wide scratch recycler.
func TestScratchRetainsFrames(t *testing.T) {
	saved := append(drainScratchBuffers(0), drainScratchBuffers(maxScratchBuf+1)...)
	t.Cleanup(func() {
		_ = drainScratchBuffers(0)
		_ = drainScratchBuffers(maxScratchBuf + 1)
		for _, buf := range saved {
			putScratch(buf)
		}
	})
	for _, size := range []int{1 << 20, 6 << 20, 9 << 20} {
		t.Run(fmt.Sprintf("MiB=%d", size>>20), func(t *testing.T) {
			c := compress.NewFrameCompressor(compress.AlgorithmZSTD, compress.LevelFast, size)
			src := bytes.Repeat([]byte{1, 3, 5, 7}, size/4)
			encoded := c.Compress(nil, src)
			kept, err := decompressKept(c, nil, encoded, int64(len(src)), true)
			require.NoError(t, err)
			require.Equal(t, src, kept)
			need := int64(len(src) + c.OutputSlack())
			buf := getScratch(need)
			require.GreaterOrEqual(t, int64(cap(buf)), need)
			if need <= maxScratchBuf {
				require.LessOrEqual(t, cap(buf), maxScratchBuf)
			} else {
				require.LessOrEqual(t, cap(buf), maxLargeScratchBuf)
			}
			putScratch(buf)
			other := bytes.Repeat([]byte{2, 4, 6, 8}, size/4)
			decoded, err := decompressKept(c, nil, c.Compress(nil, other), int64(len(other)), true)
			require.NoError(t, err)
			require.Equal(t, other, decoded)
			require.Equal(t, src, kept, "scratch is independent of kept storage")
		})
	}
	_ = drainScratchBuffers(maxScratchBuf + 1)
	putScratch(make([]byte, 0, maxLargeScratchBuf+1))
	require.Nil(t, getScratch(maxScratchBuf+1), "oversized scratch is discarded")
	putScratch(make([]byte, 0, maxScratchBuf+1))
	putScratch(make([]byte, 0, maxScratchBuf+1))
	require.NotNil(t, getScratch(maxScratchBuf+1))
	require.Nil(t, getScratch(maxScratchBuf+1), "only one large idle buffer is retained")
	require.Nil(t, getScratch(maxLargeScratchBuf+1))
}

func drainScratchBuffers(need int64) [][]byte {
	var out [][]byte
	for buf := getScratch(need); buf != nil; buf = getScratch(need) {
		out = append(out, buf)
	}
	return out
}

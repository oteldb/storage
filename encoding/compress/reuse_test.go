package compress

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoderReuseAcrossGC(t *testing.T) {
	t.Parallel()

	c := NewFrameCompressor(AlgorithmZSTD, LevelFast, 64<<10)
	src := bytes.Repeat([]byte("span attributes and values "), 100)
	want := c.Compress(nil, src)
	first := c.encoders.Get()
	require.NotNil(t, first)
	c.encoders.Put(first)
	runtime.GC()
	runtime.GC()
	require.Equal(t, want, c.Compress(nil, src))
	second := c.encoders.Get()
	require.Same(t, first, second, "a compressor keeps one idle encoder across collections")
	require.Nil(t, c.encoders.Get())
}

func TestCompressorConcurrentReuse(t *testing.T) {
	t.Parallel()

	c := NewFrameCompressor(AlgorithmZSTD, LevelFast, 64<<10)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			src := bytes.Repeat([]byte{byte(i), 1, 3, 5}, 1024)
			for range 32 {
				encoded := c.Compress(nil, src)
				decoded, err := c.Decompress(nil, encoded)
				if err != nil || !bytes.Equal(src, decoded) {
					t.Errorf("round trip failed: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
	require.NotNil(t, c.encoders.Get())
	require.Nil(t, c.encoders.Get(), "only one idle encoder is retained after concurrent calls")
}

package compress

import (
	"bytes"
	"runtime"
	"testing"
)

func BenchmarkCompressorAcrossGC(b *testing.B) {
	c := NewFrameCompressor(AlgorithmZSTD, LevelBest, 64<<10)
	src := bytes.Repeat([]byte("trace payload attributes and timestamps "), 1600)
	dst := c.Compress(nil, src)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for b.Loop() {
		runtime.GC()
		runtime.GC()
		dst = c.Compress(dst[:0], src)
	}
}

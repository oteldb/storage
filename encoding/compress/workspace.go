package compress

import (
	"unsafe"

	"github.com/pierrec/lz4/v4"
)

// EncodeWorkspace is what one encoder of c holds, borrowed by a [Compressor.Compress] call and kept
// by the pool after it: for zstd the hash tables of the level's preset and the history its window
// needs. Calls made one at a time borrow one encoder, so it is what a serial user of c holds in
// encoders.
func (c *Compressor) EncodeWorkspace() int64 {
	switch c.alg {
	case AlgorithmZSTD:
		return zstdEncodeWorkspace(c.level, c.window)
	case AlgorithmLZ4:
		return int64(unsafe.Sizeof(lz4.Compressor{}))
	default:
		return 0
	}
}

// CompressBound is the most [Compressor.Compress] appends for n bytes of input, compressed or stored
// raw: a destination with that much spare capacity takes the output without growing.
func (c *Compressor) CompressBound(n int) int {
	return n + n/255 + 64
}

// CompressTransient bounds what one [Compressor.Compress] call over n bytes allocates: into a fresh
// destination, the bound twice over for the append growth that builds it — into a presized one,
// nothing — and for lz4 the block it compresses into first.
func (c *Compressor) CompressTransient(n int, presized bool) int64 {
	bound := int64(c.CompressBound(n))

	var out int64
	if !presized {
		out = 2 * bound
	}

	if c.alg == AlgorithmLZ4 {
		out += bound
	}

	return out
}

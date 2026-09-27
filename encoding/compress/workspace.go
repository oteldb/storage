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

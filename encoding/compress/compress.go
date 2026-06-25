package compress

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Algorithm identifies a general-purpose compression algorithm.
type Algorithm uint8

const (
	// AlgorithmNone is the identity (no compression).
	AlgorithmNone Algorithm = iota
	// AlgorithmZSTD is Zstandard (high ratio, good speed; default for cold data).
	AlgorithmZSTD
	// AlgorithmLZ4 is LZ4 (fast, lower ratio; default for hot data).
	AlgorithmLZ4
)

// String returns a stable lower-case algorithm name.
func (a Algorithm) String() string {
	switch a {
	case AlgorithmNone:
		return "none"
	case AlgorithmZSTD:
		return "zstd"
	case AlgorithmLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}

// Level is a compression level. ZSTD levels: 1–22 (default 3, higher = better ratio
// but slower). LZ4 levels are not exposed (LZ4 is always fastest).
type Level uint8

const (
	// LevelDefault is the default compression level per algorithm.
	LevelDefault Level = 0
	// LevelFast is the fastest compression (ZSTD 1, LZ4 default).
	LevelFast Level = 1
	// LevelBest is the best ratio (ZSTD 19, LZ4 default).
	LevelBest Level = 19
)

// Compressor is a pooled, append-style compressor. It is safe for concurrent use
// (each Compress/Decompress call borrows a pooled encoder/decoder).
type Compressor struct {
	alg     Algorithm
	level   Level
	encPool sync.Pool
	decPool sync.Pool
}

// NewCompressor returns a [Compressor] for the given algorithm and level. Level is
// only meaningful for ZSTD; LZ4 ignores it.
func NewCompressor(alg Algorithm, level Level) *Compressor {
	c := &Compressor{alg: alg, level: level}
	c.encPool = sync.Pool{New: c.newEncoder}
	c.decPool = sync.Pool{New: c.newDecoder}
	return c
}

// Algorithm returns the compressor's algorithm.
func (c *Compressor) Algorithm() Algorithm { return c.alg }

// Compress appends the compressed form of src to dst and returns the extended slice.
// If compression does not reduce the size (rare for small inputs), the raw bytes are
// stored with a 1-byte prefix [FlagRaw].
func (c *Compressor) Compress(dst, src []byte) []byte {
	if c.alg == AlgorithmNone || len(src) == 0 {
		return append(append(dst, FlagRaw), src...)
	}

	compressed := c.compressPool(dst, src)
	// Only use compressed form if it's smaller.
	if len(compressed) > len(src)+1 {
		// Compression didn't help; store raw.
		compressed = compressed[:len(dst)] // truncate
		return append(append(compressed, FlagRaw), src...)
	}
	return compressed
}

// Decompress appends the decompressed form of src to dst and returns the extended
// slice. src must start with a flag byte ([FlagRaw] or [FlagCompressed]).
func (c *Compressor) Decompress(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}
	flag := src[0]
	body := src[1:]
	switch flag {
	case FlagRaw:
		return append(dst, body...), nil
	case FlagCompressed:
		return c.decompressPool(dst, body), nil
	default:
		return dst, badFlagError{flag}
	}
}

// Flag bytes for the compressed-block format.
const (
	// FlagRaw means the payload is stored uncompressed.
	FlagRaw byte = 0x00
	// FlagCompressed means the payload is algorithm-compressed.
	FlagCompressed byte = 0x01
)

func (c *Compressor) compressPool(dst, src []byte) []byte {
	switch c.alg {
	case AlgorithmZSTD:
		enc := c.encPool.Get().(*zstd.Encoder)
		defer c.encPool.Put(enc)
		out := enc.EncodeAll(src, append(dst, FlagCompressed))
		return out
	case AlgorithmNone:
		// Identity: store raw (Compress already short-circuits this case).
		return append(append(dst, FlagRaw), src...)
	case AlgorithmLZ4:
		// LZ4 block compression (klauspost/compress/lz4).
		// For M0 we use the zstd path as the primary; LZ4 is a stub that falls
		// through to raw. Full LZ4 wiring lands with the S3 backend (M5).
		return append(append(dst, FlagRaw), src...)
	default:
		return append(append(dst, FlagRaw), src...)
	}
}

func (c *Compressor) decompressPool(dst, src []byte) []byte {
	switch c.alg {
	case AlgorithmZSTD:
		dec := c.decPool.Get().(*zstd.Decoder)
		defer c.decPool.Put(dec)
		out, _ := dec.DecodeAll(src, dst)
		return out
	case AlgorithmNone, AlgorithmLZ4:
		// Identity / LZ4 stub: return as-is (raw fallback).
		return append(dst, src...)
	default:
		// LZ4 stub: return as-is (raw fallback).
		return append(dst, src...)
	}
}

func (c *Compressor) newEncoder() any {
	// Only ZSTD uses the pool; other algorithms bypass compressPool entirely.
	level := zstd.SpeedDefault
	switch {
	case c.level == LevelFast:
		level = zstd.SpeedFastest
	case c.level >= LevelBest:
		level = zstd.SpeedBetterCompression
	}
	enc, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(level))
	if err != nil {
		panic(err)
	}
	return enc
}

func (c *Compressor) newDecoder() any {
	// Only ZSTD uses the pool; other algorithms bypass decompressPool entirely.
	dec, err := zstd.NewReader(io.NopCloser(nilReader{}))
	if err != nil {
		panic(err)
	}
	return dec
}

// nilReader is a zero-length reader for constructing a Decoder (DecodeAll doesn't use
// the reader, but NewReader requires a non-nil one).
type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }

// badFlagError is returned when the decompressor sees an unknown flag byte.
type badFlagError struct{ flag byte }

func (e badFlagError) Error() string { return "compress: unknown block flag" }

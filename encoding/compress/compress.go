package compress

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
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
	encPool sync.Pool // *zstd.Encoder
	decPool sync.Pool // *zstd.Decoder
	lz4Pool sync.Pool // *lz4.Compressor (not safe for concurrent use, so pooled)
}

// NewCompressor returns a [Compressor] for the given algorithm and level. Level is
// only meaningful for ZSTD; LZ4 ignores it.
func NewCompressor(alg Algorithm, level Level) *Compressor {
	c := &Compressor{alg: alg, level: level}
	c.encPool = sync.Pool{New: c.newEncoder}
	c.decPool = sync.Pool{New: c.newDecoder}
	c.lz4Pool = sync.Pool{New: func() any { return &lz4.Compressor{} }}
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
		// LZ4 block compression. Body framing is [uvarint origLen][lz4 block] after the flag, so
		// Decompress can size the destination (block format carries no length itself).
		comp := c.lz4Pool.Get().(*lz4.Compressor)
		defer c.lz4Pool.Put(comp)

		block := make([]byte, lz4.CompressBlockBound(len(src)))

		n, err := comp.CompressBlock(src, block)
		if err != nil || n == 0 { // error or incompressible: store raw
			return append(append(dst, FlagRaw), src...)
		}

		dst = append(dst, FlagCompressed)
		dst = binary.AppendUvarint(dst, uint64(len(src)))

		return append(dst, block[:n]...)
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
	case AlgorithmLZ4:
		// Body is [uvarint origLen][lz4 block]. Size the destination from origLen, bounding it
		// against the block length (LZ4's max expansion is ~255×) so a malformed length cannot
		// trigger a huge allocation.
		origLen, k := binary.Uvarint(src)
		if k <= 0 || origLen > uint64(len(src))*256+1024 {
			return dst // malformed; best-effort, matching the zstd path's swallow
		}

		base := len(dst)
		dst = append(dst, make([]byte, origLen)...)

		n, err := lz4.UncompressBlock(src[k:], dst[base:])
		if err != nil {
			return dst[:base]
		}

		return dst[:base+n]
	default:
		// AlgorithmNone never produces a FlagCompressed body; return as-is defensively.
		return append(dst, src...)
	}
}

// encoderWindowBytes caps the ZSTD match window. This package only ever calls EncodeAll on a single
// block (a part column, bounded by the part size), so a huge window buys little — but klauspost sizes
// the encoder's hash tables to the window, so an unbounded one costs tens of MiB of resident state per
// encoder. 8 MiB covers a block's locality with negligible ratio loss.
const encoderWindowBytes = 8 << 20

func (c *Compressor) newEncoder() any {
	// Only ZSTD uses the pool; other algorithms bypass compressPool entirely.
	level := zstd.SpeedDefault
	switch {
	case c.level == LevelFast:
		level = zstd.SpeedFastest
	case c.level >= LevelBest:
		level = zstd.SpeedBetterCompression
	}
	// WithEncoderConcurrency(1): this pool only does one-shot EncodeAll, so the default GOMAXPROCS
	// worker set (each preallocating window+hash buffers) is pure resident waste — bounding it to one
	// cuts encoder state ~6× with no effect on ratio. WithWindowSize bounds it further (see above).
	enc, err := zstd.NewWriter(io.Discard,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(encoderWindowBytes),
	)
	if err != nil {
		panic(err)
	}
	return enc
}

func (c *Compressor) newDecoder() any {
	// Only ZSTD uses the pool; other algorithms bypass decompressPool entirely. DecodeAll never uses
	// the streaming worker pool, so bound concurrency to one and take the low-memory buffers.
	dec, err := zstd.NewReader(io.NopCloser(nilReader{}),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
	)
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

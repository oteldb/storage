package compress

import (
	"encoding/binary"
	"sync"

	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// ErrLimit is returned by [Compressor.DecompressLimit] for input that decompresses past its limit.
var ErrLimit = errors.New("compress: decompressed size exceeds the limit")

// ErrMalformed is returned by [Compressor.DecompressLimit] for input no writer of this package
// produces.
var ErrMalformed = errors.New("compress: malformed block")

const (
	// zstdMaxBlock is the most a zstd block may hold or regenerate.
	zstdMaxBlock = 128 << 10
	// zstdSlack is the output headroom past the content bound: a block's regeneration is checked
	// against the bound only after it is appended, so without it the append would grow dst.
	zstdSlack = zstdMaxBlock + 16
	// zstdNoFCSMax is the largest content klauspost writes without a frame content size.
	zstdNoFCSMax = 255
)

// DecodeWorkspace bounds what one [Compressor.DecompressLimit] call allocates beyond the
// decompressed size: the zstd output slack plus a freshly built block decoder's buffers and tables.
// LZ4 and raw blocks need none of it.
const DecodeWorkspace = 512 << 10

// OutputSlack is the spare capacity past the decompressed size that a [Compressor.DecompressLimit]
// destination of alg needs to decode without allocating. A decoded buffer keeps it for its life.
func OutputSlack(alg Algorithm) int {
	if alg == AlgorithmZSTD {
		return zstdSlack
	}

	return 0
}

// OutputSlack is [OutputSlack] for c's algorithm.
func (c *Compressor) OutputSlack() int { return OutputSlack(c.alg) }

// limitDecoders are fixed-option zstd decoders for DecompressLimit, in both builds: the cgo decoder
// has no output cap.
var limitDecoders = sync.Pool{New: func() any {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecodeAllCapLimit(true),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
	)
	if err != nil {
		panic(err)
	}

	return dec
}}

// DecompressLimit is [Compressor.Decompress] that fails with [ErrLimit] rather than produce more
// than limit bytes, and with [ErrMalformed] on input it cannot bound. It appends to dst, reusing its
// spare capacity when that covers the bound and otherwise allocating the bound exactly; dst is never
// grown by append. Allocation is at most the bound plus [DecodeWorkspace].
func (c *Compressor) DecompressLimit(dst, src []byte, limit int) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}

	limit = max(limit, 0)
	body := src[1:]

	switch src[0] {
	case FlagRaw:
		if len(body) > limit {
			return dst, errors.Wrapf(ErrLimit, "raw block of %d bytes, limit %d", len(body), limit)
		}

		return append(reserve(dst, len(body)), body...), nil
	case FlagCompressed:
		switch c.alg {
		case AlgorithmZSTD:
			return decompressZstdLimit(dst, body, limit)
		case AlgorithmLZ4:
			return decompressLZ4Limit(dst, body, limit)
		default:
			return dst, errors.Wrapf(ErrMalformed, "compressed block under %s", c.alg)
		}
	default:
		return dst, errors.Wrapf(ErrMalformed, "unknown block flag %d", src[0])
	}
}

// reserve returns dst with room for n more bytes without reallocating on append.
func reserve(dst []byte, n int) []byte {
	if cap(dst)-len(dst) >= n {
		return dst
	}

	out := make([]byte, len(dst), len(dst)+n)
	copy(out, dst)

	return out
}

func decompressLZ4Limit(dst, body []byte, limit int) ([]byte, error) {
	// A writer stores empty input raw, and the block decoder faults on an empty destination.
	origLen, k := binary.Uvarint(body)
	if k <= 0 || origLen == 0 {
		return dst, errors.Wrap(ErrMalformed, "lz4 length")
	}

	if origLen > uint64(limit) {
		return dst, errors.Wrapf(ErrLimit, "lz4 block of %d bytes, limit %d", origLen, limit)
	}

	n := int(origLen)
	out := reserve(dst, n)
	base := len(out)

	got, err := lz4.UncompressBlock(body[k:], out[base:base+n])
	if err != nil {
		return dst, errors.Wrapf(ErrMalformed, "lz4: %v", err)
	}

	if got != n {
		return dst, errors.Wrapf(ErrMalformed, "lz4 block decoded %d bytes, header says %d", got, n)
	}

	return out[:base+n], nil
}

func decompressZstdLimit(dst, body []byte, limit int) ([]byte, error) {
	bound, err := zstdContentBound(body, limit)
	if err != nil {
		return dst, err
	}

	out := reserve(dst, bound+zstdSlack)
	base := len(out)

	dec, _ := limitDecoders.Get().(*zstd.Decoder)
	res, err := dec.DecodeAll(body, out)
	limitDecoders.Put(dec)

	switch {
	case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrFrameSizeExceeded):
		return dst, errors.Wrapf(ErrLimit, "zstd: %v", err)
	case err != nil:
		return dst, errors.Wrapf(ErrMalformed, "zstd: %v", err)
	case len(res)-base > bound:
		return dst, errors.Wrapf(ErrLimit, "zstd frame decoded %d bytes, bound %d", len(res)-base, bound)
	}

	return res, nil
}

// zstdContentBound validates src as exactly one zstd frame and returns what it may decode to. The
// walk is what makes the bound hold: the decoder appends each block before checking it, so a frame
// it has not seen whole could grow the output past any capacity given to it.
func zstdContentBound(src []byte, limit int) (int, error) {
	var h zstd.Header

	rest, err := h.DecodeAndStrip(src)
	switch {
	case err != nil:
		return 0, errors.Wrapf(ErrMalformed, "zstd header: %v", err)
	case h.Skippable:
		return 0, errors.Wrap(ErrMalformed, "zstd skippable frame")
	case h.DictionaryID != 0:
		return 0, errors.Wrap(ErrMalformed, "zstd dictionary frame")
	}

	blocks := 0

	for last := false; !last; blocks++ {
		if len(rest) < 3 {
			return 0, errors.Wrap(ErrMalformed, "zstd block header truncated")
		}

		bh := uint32(rest[0]) | uint32(rest[1])<<8 | uint32(rest[2])<<16
		rest = rest[3:]
		last = bh&1 != 0
		size := int(bh >> 3)

		if size > zstdMaxBlock {
			return 0, errors.Wrapf(ErrMalformed, "zstd block of %d bytes", size)
		}

		switch (bh >> 1) & 3 {
		case 1: // RLE: one byte regenerated size times
			size = 1
		case 3:
			return 0, errors.Wrap(ErrMalformed, "zstd reserved block type")
		}

		if size > len(rest) {
			return 0, errors.Wrap(ErrMalformed, "zstd block truncated")
		}

		rest = rest[size:]
	}

	if h.HasCheckSum {
		if len(rest) < 4 {
			return 0, errors.Wrap(ErrMalformed, "zstd checksum truncated")
		}

		rest = rest[4:]
	}

	if len(rest) != 0 {
		return 0, errors.Wrapf(ErrMalformed, "zstd: %d bytes past the frame", len(rest))
	}

	if h.HasFCS {
		if h.FrameContentSize > uint64(limit) {
			return 0, errors.Wrapf(ErrLimit, "zstd frame of %d bytes, limit %d", h.FrameContentSize, limit)
		}

		return int(h.FrameContentSize), nil
	}

	// Only small content goes without a size, and then as one block: several would each append
	// unchecked.
	if blocks != 1 {
		return 0, errors.Wrapf(ErrMalformed, "zstd frame without a content size spans %d blocks", blocks)
	}

	return min(limit, zstdNoFCSMax), nil
}

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
// than limit bytes, and with [ErrMalformed] on input it cannot bound. Unlike Decompress it does not
// append: it decodes into buf's capacity, discarding buf's contents, when that covers the bound and
// otherwise into a buffer of exactly the bound. It allocates at most the bound plus
// [DecodeWorkspace].
func (c *Compressor) DecompressLimit(buf, src []byte, limit int) ([]byte, error) {
	buf = buf[:0]
	if len(src) == 0 {
		return buf, nil
	}

	limit = max(limit, 0)
	body := src[1:]

	switch src[0] {
	case FlagRaw:
		if len(body) > limit {
			return buf, errors.Wrapf(ErrLimit, "raw block of %d bytes, limit %d", len(body), limit)
		}

		return append(reserve(buf, len(body)), body...), nil
	case FlagCompressed:
		switch c.alg {
		case AlgorithmZSTD:
			return decompressZstdLimit(buf, body, limit)
		case AlgorithmLZ4:
			return decompressLZ4Limit(buf, body, limit)
		default:
			return buf, errors.Wrapf(ErrMalformed, "compressed block under %s", c.alg)
		}
	default:
		return buf, errors.Wrapf(ErrMalformed, "unknown block flag %d", src[0])
	}
}

// reserve returns an empty buf with capacity for n bytes.
func reserve(buf []byte, n int) []byte {
	if cap(buf) >= n {
		return buf[:0]
	}

	return make([]byte, 0, n)
}

// lz4MaxRatio bounds an lz4 block's expansion, just above the 255 bytes a match regenerates per
// input byte (16 MiB of zeros compresses 254.9 to 1).
const lz4MaxRatio = 256

func decompressLZ4Limit(buf, body []byte, limit int) ([]byte, error) {
	// A writer stores empty input raw, and the block decoder faults on an empty destination.
	origLen, k := binary.Uvarint(body)
	if k <= 0 || origLen == 0 {
		return buf, errors.Wrap(ErrMalformed, "lz4 length")
	}

	if origLen > uint64(limit) {
		return buf, errors.Wrapf(ErrLimit, "lz4 block of %d bytes, limit %d", origLen, limit)
	}

	if block := uint64(len(body) - k); origLen > block*lz4MaxRatio {
		return buf, errors.Wrapf(ErrMalformed, "lz4 block of %d bytes claims %d", block, origLen)
	}

	n := int(origLen)
	out := reserve(buf, n)[:n]

	got, err := lz4.UncompressBlock(body[k:], out)
	if err != nil {
		return buf, errors.Wrapf(ErrMalformed, "lz4: %v", err)
	}

	if got != n {
		return buf, errors.Wrapf(ErrMalformed, "lz4 block decoded %d bytes, header says %d", got, n)
	}

	return out, nil
}

func decompressZstdLimit(buf, body []byte, limit int) ([]byte, error) {
	bound, err := zstdContentBound(body, limit)
	if err != nil {
		return buf, err
	}

	out := reserve(buf, bound+zstdSlack)

	dec, _ := limitDecoders.Get().(*zstd.Decoder)
	res, err := dec.DecodeAll(body, out)
	limitDecoders.Put(dec)

	switch {
	case errors.Is(err, zstd.ErrDecoderSizeExceeded), errors.Is(err, zstd.ErrFrameSizeExceeded):
		return buf, errors.Wrapf(ErrLimit, "zstd: %v", err)
	case err != nil:
		return buf, errors.Wrapf(ErrMalformed, "zstd: %v", err)
	case len(res) > bound:
		return buf, errors.Wrapf(ErrLimit, "zstd frame decoded %d bytes, bound %d", len(res), bound)
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

	// A block regenerates at most zstdMaxBlock, so a content size past that per block is a lie, and
	// capping it here keeps the bound plus slack from overflowing.
	if h.HasFCS {
		switch {
		case h.FrameContentSize > uint64(limit):
			return 0, errors.Wrapf(ErrLimit, "zstd frame of %d bytes, limit %d", h.FrameContentSize, limit)
		case h.FrameContentSize > uint64(blocks)*zstdMaxBlock:
			return 0, errors.Wrapf(ErrMalformed, "zstd frame of %d blocks claims %d bytes", blocks, h.FrameContentSize)
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

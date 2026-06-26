package chunk

import (
	"errors"

	"github.com/oteldb/storage/encoding/bitstream"
)

// Codec identifies a column encoding (DESIGN.md §6, §14 M0). It is stored in the
// column/part metadata so the decoder selects the right [Decode] function. Values
// are stable (persisted/wire-stable); never reorder.
type Codec uint8

const (
	// CodecNone is the identity codec (raw values, no compression).
	CodecNone Codec = iota
	// CodecDoD is delta-of-delta for timestamps (Prometheus-style, ms-resolution
	// buckets 14/17/20/64). ~0 bits/sample at constant scrape interval.
	CodecDoD
	// CodecGorilla is Gorilla XOR for float64 values. ~1 bit/sample when consecutive
	// float values share leading/trailing zero structure.
	CodecGorilla
	// CodecDict is dictionary encoding for low-cardinality strings (≤256 distinct →
	// 1 byte/row).
	CodecDict
	// CodecT64 is ClickHouse T64: bit-transpose + crop for low-range int64 values.
	// Lossless; crops unused high bits, transposes 64-wide blocks.
	CodecT64
	// CodecDecimal is VictoriaMetrics-style scaled-decimal + nearest-delta for
	// float64. Optionally lossy (precisionBits < 64): zeros trailing low bits of
	// deltas so the varint stream is ZSTD-compressible.
	CodecDecimal
	// CodecID128 is run-length encoding for 128-bit id columns ([U128]): a sorted id
	// column (e.g. the SeriesID sort key of a metric part) is long runs of one id, so
	// RLE stores a distinct id + run length per run.
	CodecID128
	// CodecBytesRaw stores byte-string columns with no dictionary: a fixed-width block
	// (one shared length + values back-to-back) when every value is the same length, else
	// length-prefixed inline values. It is the right choice for high-cardinality, near-unique
	// id columns (e.g. a span id) where a dictionary is pure overhead. The decoder reads the
	// self-describing flag in the stream, so [DecodeBytes]/[DecodeBytesDict] handle it without
	// consulting the codec.
	CodecBytesRaw
)

// String returns a stable lower-case codec name.
func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecDoD:
		return "dod"
	case CodecGorilla:
		return "gorilla"
	case CodecDict:
		return "dict"
	case CodecT64:
		return "t64"
	case CodecDecimal:
		return "decimal"
	case CodecID128:
		return "id128"
	case CodecBytesRaw:
		return "bytesraw"
	default:
		return "unknown"
	}
}

// header is the per-column-stream prefix written by every [Encode] function and
// consumed by every [Decode] function: a uvarint row count followed by the codec
// payload. It is internal — callers pick the codec by column metadata, then call
// the matching Encode/Decode pair.

// writeHeader appends the row count as a uvarint to dst via a pooled bitstream
// writer, then returns the writer ready for the codec payload.
func writeHeader(dst []byte, rows int) (*bitstream.Writer, []byte) {
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(rows))

	b := w.Bytes()

	return w, b
}

// readHeader reads the row count from src and returns a reader positioned after it,
// along with the number of bytes consumed by the uvarint count.
func readHeader(src []byte) (r *bitstream.Reader, rows, consumed int, err error) {
	rows, consumed, err = readHeaderInto(src, nil)
	if err != nil {
		return nil, 0, 0, err
	}

	return bitstream.NewReader(src[consumed:]), rows, consumed, nil
}

func readHeaderInto(src []byte, r *bitstream.Reader) (rows, consumed int, err error) {
	// Read the uvarint count directly from the bytes (it's byte-aligned).
	var (
		n int
		u uint64
	)
	{
		br := &byteReader{data: src}

		u, err = readUvarint(br)
		if err != nil {
			return 0, 0, err
		}

		n = br.off
	}

	if r != nil {
		r.Reset(src[n:])
	}

	return int(u), n, nil
}

// byteReader is a minimal io.ByteReader for the header uvarint, avoiding the
// bitstream.Reader overhead before we know the count.
type byteReader struct {
	data []byte
	off  int
}

func (r *byteReader) ReadByte() (byte, error) {
	if r.off >= len(r.data) {
		return 0, errEOF
	}

	b := r.data[r.off]
	r.off++

	return b, nil
}

// readUvarint mirrors bitstream.Reader.ReadUvarint but over a byteReader.
func readUvarint(r *byteReader) (uint64, error) {
	var (
		x uint64
		s uint
	)

	for i := range 10 {
		b, err := r.ReadByte()
		if err != nil {
			if i == 0 {
				return 0, errEOF
			}

			return x, errEOF
		}

		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}

		x |= uint64(b&0x7f) << s
		s += 7
	}

	return x, errUnexpectedEOF
}

// Sentinel errors (not exported; codecs wrap them with context).
var (
	errEOF           = eofError{}
	errUnexpectedEOF = unexpectedEOFError{}
)

type eofError struct{}

func (eofError) Error() string { return "chunk: unexpected end of stream" }

type unexpectedEOFError struct{}

func (unexpectedEOFError) Error() string { return "chunk: truncated stream" }

// IsEOF reports whether err is an end-of-stream error.
func IsEOF(err error) bool {
	var eofError eofError
	ok := errors.As(err, &eofError)

	return ok
}

// maxColumnRows is the defensive row-count ceiling for codecs whose row count is NOT bounded by the
// stream length — a constant T64 column (only a 16-byte header) and a U128 RLE column (a run packs
// many rows into a few bytes). It is a panic guard against a corrupt header requesting a giant make,
// far above any real column; functional row counts are bounded by the embedder's flush thresholds,
// and column streams come from CRC-validated parts.
const maxColumnRows = 1 << 31

// boundRows rejects, as a corrupt stream, a decoded row count that is negative — an int(uvarint)
// that overflowed — or exceeds maxRows, the largest count the remaining stream could possibly
// encode. Guarding before a decoder sizes its output slice keeps a corrupt header from triggering a
// huge allocation. For bit-packed columns (DoD, Gorilla, T64 value blocks) every row consumes at
// least one bit, so the caller passes maxRows = 8 × remaining bytes; for the unbounded-by-length
// codecs the caller passes [maxColumnRows].
func boundRows(rows, maxRows int) error {
	if rows < 0 || rows > maxRows {
		return errUnexpectedEOF
	}

	return nil
}

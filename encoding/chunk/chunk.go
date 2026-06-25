package chunk

import "github.com/oteldb/storage/encoding/bitstream"

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
	return w, w.Bytes()
}

// readHeader reads the row count from src and returns a reader positioned after it,
// along with the number of bytes consumed by the uvarint count.
func readHeader(src []byte) (r *bitstream.Reader, rows int, consumed int, err error) {
	rows, consumed, err = readHeaderInto(src, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	return bitstream.NewReader(src[consumed:]), rows, consumed, nil
}

func readHeaderInto(src []byte, r *bitstream.Reader) (rows int, consumed int, err error) {
	// Read the uvarint count directly from the bytes (it's byte-aligned).
	var n int
	var u uint64
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
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
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
	_, ok := err.(eofError)
	return ok
}

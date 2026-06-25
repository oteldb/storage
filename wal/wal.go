package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Record type tags. New types may be appended; unknown types are skipped on replay so
// an old reader tolerates a newer log.
const (
	recordSeries byte = 1
)

// seriesIDLen is the fixed 16-byte big-endian width of a [signal.SeriesID] on the wire.
const seriesIDLen = 16

// ErrCorrupt is returned when a complete record fails its CRC check.
var ErrCorrupt = errors.New("wal: corrupt record")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Handlers receives decoded records during [Replay]. Unset handlers skip their record
// type. More handlers (samples, …) are added as later milestones add record types.
type Handlers struct {
	OnSeries func(id signal.SeriesID, attrs signal.Attributes) error
}

// Writer appends framed records to an [io.Writer] (typically a segment file). It reuses
// internal buffers, so it is not safe for concurrent use.
type Writer struct {
	w       io.Writer
	payload []byte
	frame   []byte
}

// NewWriter returns a [Writer] over w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteSeries logs a series registration: its [signal.SeriesID] and full typed attribute
// set. Replaying it reconstructs the series in the index.
func (wr *Writer) WriteSeries(id signal.SeriesID, attrs signal.Attributes) error {
	wr.payload = attrs.AppendHashInput(id.AppendBinary(wr.payload[:0]))
	wr.frame = appendFrame(wr.frame[:0], recordSeries, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

// Replay reads every complete record from data and dispatches it to h. It stops cleanly
// at end-of-log or a torn final record (returning nil), and returns an
// [ErrCorrupt]-wrapping error on a complete record whose CRC fails. Records already
// applied before the stopping point are kept.
func Replay(data []byte, h Handlers) error {
	for off := 0; off < len(data); {
		typ, payload, n, err := readFrame(data[off:])
		if errors.Is(err, io.EOF) {
			return nil // clean end or torn tail
		}

		if err != nil {
			return err
		}

		off += n

		if err := dispatch(typ, payload, h); err != nil {
			return err
		}
	}

	return nil
}

func dispatch(typ byte, payload []byte, h Handlers) error {
	switch typ {
	case recordSeries:
		if h.OnSeries == nil {
			return nil
		}

		id, attrs, err := parseSeries(payload)
		if err != nil {
			return err
		}

		return h.OnSeries(id, attrs)
	default:
		return nil // unknown record type: skip for forward compatibility
	}
}

// appendFrame appends [uvarint bodyLen][type][payload][u32 CRC32C(body)] to dst.
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(1+len(payload)))
	start := len(dst)
	dst = append(dst, typ)
	dst = append(dst, payload...)
	crc := crc32.Checksum(dst[start:], castagnoli)

	return binary.BigEndian.AppendUint32(dst, crc)
}

// readFrame reads one frame from the front of src. It returns io.EOF when src is empty
// or holds only an incomplete (torn) frame, and [ErrCorrupt] when a complete frame fails
// its CRC. The returned payload aliases src.
func readFrame(src []byte) (typ byte, payload []byte, consumed int, err error) {
	bodyLen, n := binary.Uvarint(src)
	if n <= 0 || bodyLen == 0 {
		return 0, nil, 0, io.EOF
	}

	// Bound bodyLen against the bytes available (in uint64, to avoid int overflow)
	// before computing any offset: a complete frame needs body + a 4-byte CRC.
	avail := len(src) - n
	if avail < 4 || bodyLen > uint64(avail-4) {
		return 0, nil, 0, io.EOF // torn tail
	}

	bodyEnd := n + int(bodyLen)
	crcEnd := bodyEnd + 4
	body := src[n:bodyEnd]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(src[bodyEnd:crcEnd]) {
		return 0, nil, 0, errors.Wrap(ErrCorrupt, "CRC mismatch")
	}

	return body[0], body[1:], crcEnd, nil
}

// parseSeries decodes a series record payload: a 16-byte SeriesID then the typed
// attribute set. The returned attributes alias payload.
func parseSeries(payload []byte) (signal.SeriesID, signal.Attributes, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "series record too short")
	}

	id := signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}

	attrs, _, err := signal.DecodeAttributes(payload[seriesIDLen:])
	if err != nil {
		return signal.SeriesID{}, nil, errors.Wrap(err, "series attributes")
	}

	return id, attrs, nil
}

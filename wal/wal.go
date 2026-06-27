package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Record type tags. New types may be appended; unknown types are skipped on replay so
// an old reader tolerates a newer log.
const (
	recordSeries    byte = 1
	recordSamples   byte = 2
	recordRecords   byte = 3 // generic, engine-encoded record payload (logs, traces, …)
	recordSide      byte = 4 // opaque, engine-encoded side-store delta (profiles symbol store)
	recordSamplesSF byte = 5 // samples that also carry a per-sample lossy-sampling scale factor
)

// seriesIDLen is the fixed 16-byte big-endian width of a [signal.SeriesID] on the wire.
const seriesIDLen = 16

// ErrCorrupt is returned when a complete record fails its CRC check.
var ErrCorrupt = errors.New("wal: corrupt record")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Handlers receives decoded records during [Replay]. Unset handlers skip their record
// type.
type Handlers struct {
	OnSeries  func(id signal.SeriesID, s signal.Series) error
	OnSamples func(id signal.SeriesID, ts []int64, values []float64) error
	// OnSamplesSF receives samples that carry per-sample lossy-sampling scale factors (len(sf) ==
	// len(ts)). If unset, such records fall back to OnSamples (the weights are dropped, i.e. read as
	// 1) so a reader that does not care about sampling still recovers the samples.
	OnSamplesSF func(id signal.SeriesID, ts []int64, values, sf []float64) error
	OnRecords   func(id signal.SeriesID, payload []byte) error
	OnSide      func(payload []byte) error
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

// WriteSeries logs a series registration: its [signal.SeriesID] and full identity
// (Resource + Scope + data-point attributes). Replaying it reconstructs the series.
func (wr *Writer) WriteSeries(id signal.SeriesID, s signal.Series) error {
	wr.payload = s.AppendHashInput(id.AppendBinary(wr.payload[:0]))
	wr.frame = appendFrame(wr.frame[:0], recordSeries, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

// WriteSamples logs a run of samples for one series: its [signal.SeriesID] then the
// (timestamp, value) pairs. ts and values must have the same length.
func (wr *Writer) WriteSamples(id signal.SeriesID, ts []int64, values []float64) error {
	wr.payload = appendSamples(id.AppendBinary(wr.payload[:0]), ts, values)
	wr.frame = appendFrame(wr.frame[:0], recordSamples, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

// WriteSamplesSF logs a run of samples for one series that also carry per-sample lossy-sampling
// scale factors: its [signal.SeriesID] then the (timestamp, value, sf) triples. ts, values, and sf
// must have the same length. Used only when sampling actually weighted the batch; the unsampled path
// stays on [Writer.WriteSamples] (no per-sample sf on the wire).
func (wr *Writer) WriteSamplesSF(id signal.SeriesID, ts []int64, values, sf []float64) error {
	wr.payload = appendSamplesSF(id.AppendBinary(wr.payload[:0]), ts, values, sf)
	wr.frame = appendFrame(wr.frame[:0], recordSamplesSF, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

// WriteSide logs an opaque engine-encoded side-store delta (e.g. a profiles symbol-store delta).
// It carries no series id — the payload is self-describing to the engine that wrote it.
func (wr *Writer) WriteSide(payload []byte) error {
	wr.frame = appendFrame(wr.frame[:0], recordSide, payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

func appendSamples(dst []byte, ts []int64, values []float64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ts)))
	for i := range ts {
		dst = binary.AppendVarint(dst, ts[i])
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(values[i]))
	}

	return dst
}

func appendSamplesSF(dst []byte, ts []int64, values, sf []float64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ts)))
	for i := range ts {
		dst = binary.AppendVarint(dst, ts[i])
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(values[i]))
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(sf[i]))
	}

	return dst
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

		id, s, err := parseSeries(payload)
		if err != nil {
			return err
		}

		return h.OnSeries(id, s)
	case recordSamples:
		if h.OnSamples == nil {
			return nil
		}

		id, ts, values, err := parseSamples(payload)
		if err != nil {
			return err
		}

		return h.OnSamples(id, ts, values)
	case recordSamplesSF:
		id, ts, values, sf, err := parseSamplesSF(payload)
		if err != nil {
			return err
		}

		// Prefer the sf-aware handler; fall back to the plain one (dropping the weights) so a
		// reader that does not track sampling still recovers the samples.
		switch {
		case h.OnSamplesSF != nil:
			return h.OnSamplesSF(id, ts, values, sf)
		case h.OnSamples != nil:
			return h.OnSamples(id, ts, values)
		default:
			return nil
		}
	case recordRecords:
		if h.OnRecords == nil {
			return nil
		}

		id, blob, err := parseRecords(payload)
		if err != nil {
			return err
		}

		return h.OnRecords(id, blob)
	case recordSide:
		if h.OnSide == nil {
			return nil
		}

		return h.OnSide(payload)
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

// parseSeries decodes a series record payload: a 16-byte SeriesID then the full identity
// (Resource + Scope + attributes). The returned identity aliases payload.
func parseSeries(payload []byte) (signal.SeriesID, signal.Series, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, signal.Series{}, errors.Wrap(ErrCorrupt, "series record too short")
	}

	id := signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}

	s, _, err := signal.DecodeSeries(payload[seriesIDLen:])
	if err != nil {
		return signal.SeriesID{}, signal.Series{}, errors.Wrap(err, "series identity")
	}

	return id, s, nil
}

// parseSamples decodes a samples record payload: a 16-byte SeriesID then a uvarint count
// and that many (varint timestamp, float64 value) pairs. Bounds-checked; never panics.
func parseSamples(payload []byte) (signal.SeriesID, []int64, []float64, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, nil, nil, errors.Wrap(ErrCorrupt, "samples record too short")
	}

	id := signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}

	rest := payload[seriesIDLen:]

	n, k := binary.Uvarint(rest)
	if k <= 0 || n > uint64(len(rest)) { // ≥1 byte per sample guards against OOM
		return signal.SeriesID{}, nil, nil, errors.Wrap(ErrCorrupt, "sample count")
	}

	off := k
	ts := make([]int64, 0, n)
	values := make([]float64, 0, n)

	for range n {
		t, kt := binary.Varint(rest[off:])
		if kt <= 0 {
			return signal.SeriesID{}, nil, nil, errors.Wrap(ErrCorrupt, "sample timestamp")
		}

		off += kt
		if len(rest)-off < 8 {
			return signal.SeriesID{}, nil, nil, errors.Wrap(ErrCorrupt, "sample value")
		}

		values = append(values, math.Float64frombits(binary.BigEndian.Uint64(rest[off:])))
		off += 8
		ts = append(ts, t)
	}

	return id, ts, values, nil
}

// parseSamplesSF decodes a sampled-samples record: a 16-byte SeriesID then a uvarint count and that
// many (varint timestamp, float64 value, float64 sf) triples. Bounds-checked; never panics.
func parseSamplesSF(payload []byte) (signal.SeriesID, []int64, []float64, []float64, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, nil, nil, nil, errors.Wrap(ErrCorrupt, "samples record too short")
	}

	id := signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}

	rest := payload[seriesIDLen:]

	n, k := binary.Uvarint(rest)
	if k <= 0 || n > uint64(len(rest)) { // ≥1 byte per sample guards against OOM
		return signal.SeriesID{}, nil, nil, nil, errors.Wrap(ErrCorrupt, "sample count")
	}

	off := k
	ts := make([]int64, 0, n)
	values := make([]float64, 0, n)
	sf := make([]float64, 0, n)

	for range n {
		t, kt := binary.Varint(rest[off:])
		if kt <= 0 {
			return signal.SeriesID{}, nil, nil, nil, errors.Wrap(ErrCorrupt, "sample timestamp")
		}

		off += kt
		if len(rest)-off < 16 { // an 8-byte value plus an 8-byte sf
			return signal.SeriesID{}, nil, nil, nil, errors.Wrap(ErrCorrupt, "sample value/sf")
		}

		values = append(values, math.Float64frombits(binary.BigEndian.Uint64(rest[off:])))
		off += 8
		sf = append(sf, math.Float64frombits(binary.BigEndian.Uint64(rest[off:])))
		off += 8
		ts = append(ts, t)
	}

	return id, ts, values, sf, nil
}

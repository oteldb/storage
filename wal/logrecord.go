package wal

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// LogRecord is the WAL wire form of one log record: the per-record columns of the logs vertical.
// It is signal-neutral (it does not import the logs model) — Attrs holds the record's attributes
// already serialized (the reversible [signal.Attributes] encoding), opaque to the WAL. The logs
// engine maps its head buffers to and from this struct.
type LogRecord struct {
	Timestamp         int64
	ObservedTimestamp int64
	SeverityNumber    int32
	Flags             uint32
	Dropped           uint32
	SeverityText      []byte
	Body              []byte
	TraceID           []byte
	SpanID            []byte
	Attrs             []byte // pre-serialized attributes (opaque)
}

// WriteLogRecords logs a run of log records for one stream: its [signal.SeriesID] then the
// records. Replaying it appends the records to the stream's head buffer.
func (wr *Writer) WriteLogRecords(id signal.SeriesID, recs []LogRecord) error {
	wr.payload = appendLogRecords(id.AppendBinary(wr.payload[:0]), recs)
	wr.frame = appendFrame(wr.frame[:0], recordLogRecords, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

func appendLogRecords(dst []byte, recs []LogRecord) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(recs)))
	for i := range recs {
		r := &recs[i]
		dst = binary.AppendVarint(dst, r.Timestamp)
		dst = binary.AppendVarint(dst, r.ObservedTimestamp)
		dst = binary.AppendVarint(dst, int64(r.SeverityNumber))
		dst = binary.AppendUvarint(dst, uint64(r.Flags))
		dst = binary.AppendUvarint(dst, uint64(r.Dropped))
		dst = appendBytes(dst, r.SeverityText)
		dst = appendBytes(dst, r.Body)
		dst = appendBytes(dst, r.TraceID)
		dst = appendBytes(dst, r.SpanID)
		dst = appendBytes(dst, r.Attrs)
	}

	return dst
}

// parseLogRecords decodes a log-records payload: a 16-byte SeriesID then a uvarint count and that
// many records. Fully bounds-checked; never panics. The returned byte fields alias payload.
func parseLogRecords(payload []byte) (signal.SeriesID, []LogRecord, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record too short")
	}

	id := signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}

	rest := payload[seriesIDLen:]

	n, k := binary.Uvarint(rest)
	if k <= 0 || n > uint64(len(rest)) { // ≥1 byte per record guards against OOM
		return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record count")
	}

	off := k
	recs := make([]LogRecord, 0, n)

	for range n {
		var r LogRecord

		for _, field := range []*int64{&r.Timestamp, &r.ObservedTimestamp} {
			v, w := binary.Varint(rest[off:])
			if w <= 0 {
				return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record timestamp")
			}

			*field = v
			off += w
		}

		sev, w := binary.Varint(rest[off:])
		if w <= 0 {
			return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record severity")
		}

		r.SeverityNumber = int32(sev)
		off += w

		flags, w := binary.Uvarint(rest[off:])
		if w <= 0 {
			return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record flags")
		}

		r.Flags = uint32(flags)
		off += w

		dropped, w := binary.Uvarint(rest[off:])
		if w <= 0 {
			return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "log record dropped")
		}

		r.Dropped = uint32(dropped)
		off += w

		for _, field := range []*[]byte{&r.SeverityText, &r.Body, &r.TraceID, &r.SpanID, &r.Attrs} {
			b, w, err := takeBytes(rest[off:])
			if err != nil {
				return signal.SeriesID{}, nil, err
			}

			*field = b
			off += w
		}

		recs = append(recs, r)
	}

	return id, recs, nil
}

// appendBytes appends a uvarint length then the bytes.
func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))

	return append(dst, b...)
}

// takeBytes reads a uvarint length then that many bytes from the front of src, bounds-checked.
// The returned slice aliases src; a zero-length field returns nil.
func takeBytes(src []byte) ([]byte, int, error) {
	l, n := binary.Uvarint(src)
	if n <= 0 || l > uint64(len(src)-n) {
		return nil, 0, errors.Wrap(ErrCorrupt, "log record bytes")
	}

	if l == 0 {
		return nil, n, nil
	}

	return src[n : n+int(l)], n + int(l), nil
}

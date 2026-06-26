package wal

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// decodeSeriesID reads the fixed 16-byte big-endian id at the front of payload.
func decodeSeriesID(payload []byte) signal.SeriesID {
	return signal.SeriesID{
		Hi: binary.BigEndian.Uint64(payload[:8]),
		Lo: binary.BigEndian.Uint64(payload[8:seriesIDLen]),
	}
}

// WriteRecords logs a run of records for one stream: its [signal.SeriesID] then an opaque,
// engine-encoded payload (the record engine owns the column encoding, keyed by its schema; the WAL
// stays signal-agnostic). Replaying it hands the payload back to the engine via [Handlers.OnRecords].
func (wr *Writer) WriteRecords(id signal.SeriesID, payload []byte) error {
	wr.payload = append(id.AppendBinary(wr.payload[:0]), payload...)
	wr.frame = appendFrame(wr.frame[:0], recordRecords, wr.payload)

	if _, err := wr.w.Write(wr.frame); err != nil {
		return errors.Wrap(err, "write record")
	}

	return nil
}

// parseRecords splits a records frame into the stream id and the opaque payload (aliasing the
// frame). Bounds-checked; never panics.
func parseRecords(payload []byte) (signal.SeriesID, []byte, error) {
	if len(payload) < seriesIDLen {
		return signal.SeriesID{}, nil, errors.Wrap(ErrCorrupt, "records frame too short")
	}

	id := decodeSeriesID(payload)

	return id, payload[seriesIDLen:], nil
}

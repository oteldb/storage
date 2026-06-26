package trace

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Events and links are serialized into per-span byte columns (the engine treats them opaquely).
// The encodings are self-describing and reversible via [DecodeEvents]/[DecodeLinks], so an embedder
// reconstructs them from a fetched span's events/links columns. Attributes use the reversible
// [signal.Attributes] encoding.

// encodeEvents serializes a span's events: a uvarint count, then per event the time (varint), the
// name, the serialized attributes, and the dropped-attribute count (uvarint).
func encodeEvents(evs []Event) []byte {
	if len(evs) == 0 {
		return nil
	}

	dst := binary.AppendUvarint(nil, uint64(len(evs)))
	for i := range evs {
		e := &evs[i]
		dst = binary.AppendVarint(dst, e.Time)
		dst = appendBytes(dst, e.Name)
		dst = appendBytes(dst, e.Attributes.AppendHashInput(nil))
		dst = binary.AppendUvarint(dst, uint64(e.Dropped))
	}

	return dst
}

// DecodeEvents parses [encodeEvents] output. Bounds-checked; byte fields alias data.
func DecodeEvents(data []byte) ([]Event, error) {
	if len(data) == 0 {
		return nil, nil
	}

	n, off := binary.Uvarint(data)
	if off <= 0 || n > uint64(len(data)) {
		return nil, errors.Wrap(signal.ErrMalformed, "event count")
	}

	out := make([]Event, 0, n)

	for range n {
		var e Event

		t, w := binary.Varint(data[off:])
		if w <= 0 {
			return nil, errors.Wrap(signal.ErrMalformed, "event time")
		}

		e.Time = t
		off += w

		var err error
		if e.Name, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		var attrs []byte
		if attrs, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		if len(attrs) > 0 {
			if e.Attributes, _, err = signal.DecodeAttributes(attrs); err != nil {
				return nil, err
			}
		}

		d, w := binary.Uvarint(data[off:])
		if w <= 0 {
			return nil, errors.Wrap(signal.ErrMalformed, "event dropped")
		}

		e.Dropped = uint32(d)
		off += w

		out = append(out, e)
	}

	return out, nil
}

// encodeLinks serializes a span's links: a uvarint count, then per link the trace id, span id,
// trace state, serialized attributes, and dropped count.
func encodeLinks(ls []Link) []byte {
	if len(ls) == 0 {
		return nil
	}

	dst := binary.AppendUvarint(nil, uint64(len(ls)))
	for i := range ls {
		l := &ls[i]
		dst = appendBytes(dst, l.TraceID)
		dst = appendBytes(dst, l.SpanID)
		dst = appendBytes(dst, l.TraceState)
		dst = appendBytes(dst, l.Attributes.AppendHashInput(nil))
		dst = binary.AppendUvarint(dst, uint64(l.Dropped))
	}

	return dst
}

// DecodeLinks parses [encodeLinks] output. Bounds-checked; byte fields alias data.
func DecodeLinks(data []byte) ([]Link, error) {
	if len(data) == 0 {
		return nil, nil
	}

	n, off := binary.Uvarint(data)
	if off <= 0 || n > uint64(len(data)) {
		return nil, errors.Wrap(signal.ErrMalformed, "link count")
	}

	out := make([]Link, 0, n)

	for range n {
		var l Link

		var err error
		if l.TraceID, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		if l.SpanID, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		if l.TraceState, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		var attrs []byte
		if attrs, off, err = takeBytes(data, off); err != nil {
			return nil, err
		}

		if len(attrs) > 0 {
			if l.Attributes, _, err = signal.DecodeAttributes(attrs); err != nil {
				return nil, err
			}
		}

		d, w := binary.Uvarint(data[off:])
		if w <= 0 {
			return nil, errors.Wrap(signal.ErrMalformed, "link dropped")
		}

		l.Dropped = uint32(d)
		off += w

		out = append(out, l)
	}

	return out, nil
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))

	return append(dst, b...)
}

// takeBytes reads a uvarint length then that many bytes from data at off, returning the new offset.
func takeBytes(data []byte, off int) ([]byte, int, error) {
	l, w := binary.Uvarint(data[off:])
	if w <= 0 || l > uint64(len(data)-off-w) {
		return nil, 0, errors.Wrap(signal.ErrMalformed, "length-prefixed bytes")
	}

	off += w
	if l == 0 {
		return nil, off, nil
	}

	return data[off : off+int(l)], off + int(l), nil
}

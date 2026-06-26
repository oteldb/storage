package recordengine

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

// The WAL/replication payload for a stream's records is an opaque blob the engine encodes from its
// schema (the WAL itself stays signal-agnostic — it frames stream-id ‖ blob). The blob is a uvarint
// count followed, per record, by the timestamp (varint), the int columns (varint each, schema
// order), then the byte columns (length-prefixed, schema order). encode/decode agree on the column
// counts via the schema.

// cloneRec deep-copies r so it can outlive the caller's scratch (used to stage WAL records).
func cloneRec(r rec) rec {
	ints := make([]int64, len(r.ints))
	copy(ints, r.ints)

	byts := make([][]byte, len(r.bytes))
	for k := range r.bytes {
		byts[k] = cloneBytes(r.bytes[k])
	}

	return rec{ts: r.ts, ints: ints, bytes: byts}
}

func encodeRecs(recs []rec) []byte {
	dst := binary.AppendUvarint(nil, uint64(len(recs)))
	for i := range recs {
		r := &recs[i]
		dst = binary.AppendVarint(dst, r.ts)

		for _, v := range r.ints {
			dst = binary.AppendVarint(dst, v)
		}

		for _, b := range r.bytes {
			dst = binary.AppendUvarint(dst, uint64(len(b)))
			dst = append(dst, b...)
		}
	}

	return dst
}

// decodeRecs parses encodeRecs output for a schema with numInts int columns and numBytes byte
// columns. Fully bounds-checked; never panics. Byte fields alias data.
func decodeRecs(data []byte, numInts, numBytes int) ([]rec, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 || count > uint64(len(data)) {
		return nil, errors.Wrap(errMalformed, "record count")
	}

	off := n
	recs := make([]rec, 0, count)

	for range count {
		ts, w := binary.Varint(data[off:])
		if w <= 0 {
			return nil, errors.Wrap(errMalformed, "record timestamp")
		}

		off += w

		r := rec{ts: ts, ints: make([]int64, numInts), bytes: make([][]byte, numBytes)}

		for k := range r.ints {
			v, w := binary.Varint(data[off:])
			if w <= 0 {
				return nil, errors.Wrap(errMalformed, "record int")
			}

			r.ints[k] = v
			off += w
		}

		for k := range r.bytes {
			l, w := binary.Uvarint(data[off:])
			if w <= 0 || l > uint64(len(data)-off-w) {
				return nil, errors.Wrap(errMalformed, "record bytes")
			}

			off += w
			if l > 0 {
				r.bytes[k] = data[off : off+int(l)]
				off += int(l)
			}
		}

		recs = append(recs, r)
	}

	return recs, nil
}

var errMalformed = errors.New("recordengine: malformed record payload")

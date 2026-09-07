// Package watermark encodes a part's per-series durability watermark sidecar: the newest timestamp
// each series has in that part.
//
// Both engines write it beside the part at flush and merge, and a replica refresh reads it instead
// of decoding the part's whole timestamp column. It is the same shape in both — the record engine's
// streams are [signal.SeriesID] too — so the codec lives here rather than twice.
package watermark

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Entry is one series' newest timestamp in a part.
type Entry struct {
	ID  signal.SeriesID
	Max int64
}

const magic uint32 = 0x4F54574D // "OTWM"

// entryWidth is one encoded entry: id hi, id lo, max.
const entryWidth = 24

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt marks an unreadable sidecar. A reader treats it like an absent one and falls back to
// decoding the timestamp column.
var ErrCorrupt = errors.New("watermark: corrupt sidecar")

// Key is the backend key of a part's watermark sidecar (deleted with the part, since a part is
// removed by listing its prefix).
func Key(prefix string) string { return prefix + "/smax" }

// Encode serializes the entries in the caller's order: [magic][uvarint n] then per entry
// [u64 id.Hi][u64 id.Lo][i64 max], all big-endian, with a trailing CRC32C. Encode appends, and the
// checksum covers only the bytes it appended — whatever dst already held is not part of the record.
func Encode(dst []byte, entries []Entry) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, magic)
	dst = binary.AppendUvarint(dst, uint64(len(entries)))

	for _, e := range entries {
		dst = binary.BigEndian.AppendUint64(dst, e.ID.Hi)
		dst = binary.BigEndian.AppendUint64(dst, e.ID.Lo)
		dst = binary.BigEndian.AppendUint64(dst, uint64(e.Max))
	}

	return binary.BigEndian.AppendUint32(dst, crc32.Checksum(dst[start:], crcTable))
}

// Decode parses a sidecar. It bounds-checks every field and never panics, returning [ErrCorrupt] on
// any malformed input so the caller can fall back to decoding.
func Decode(data []byte) ([]Entry, error) {
	if len(data) < 8 {
		return nil, errors.Wrap(ErrCorrupt, "short")
	}

	body := data[:len(data)-4]
	if crc32.Checksum(body, crcTable) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return nil, errors.Wrap(ErrCorrupt, "crc")
	}

	if binary.BigEndian.Uint32(body) != magic {
		return nil, errors.Wrap(ErrCorrupt, "magic")
	}

	rest := body[4:]

	n, m := binary.Uvarint(rest)
	if m <= 0 {
		return nil, errors.Wrap(ErrCorrupt, "count")
	}
	rest = rest[m:]

	// Checked against the remaining bytes before allocating, so a bogus count cannot make the
	// reader allocate for entries the object does not carry.
	if len(rest)%entryWidth != 0 || uint64(len(rest)/entryWidth) != n {
		return nil, errors.Wrap(ErrCorrupt, "length")
	}

	out := make([]Entry, 0, n)
	for len(rest) > 0 {
		out = append(out, Entry{
			ID:  signal.SeriesID{Hi: binary.BigEndian.Uint64(rest[:8]), Lo: binary.BigEndian.Uint64(rest[8:16])},
			Max: int64(binary.BigEndian.Uint64(rest[16:24])),
		})
		rest = rest[entryWidth:]
	}

	return out, nil
}

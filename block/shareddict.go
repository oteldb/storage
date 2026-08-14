package block

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/pool"
)

// Shared-dictionary bytes columns. A block-framed bytes column whose granules each carry their own
// dictionary loses every repeat that spans a granule boundary. Measured on a real log corpus that
// costs +74% on the record attributes column, whose blobs repeat across the whole part, against
// +6% on the message body, whose values are near-unique. The dictionary has to be able to span
// granules for the first column while getting out of the way for the second.
//
// So the dictionary is written once for the column and each granule holds only ids into it —
// ClickHouse's LowCardinality shape, where the dictionary spans granules and rolls over only when
// it outgrows its id width. A granule that does not benefit (its values are near-unique, so the
// shared dictionary would grow by a full granule's worth of entries to serve one granule) opts out
// and self-encodes exactly as before, which is the plain-String path ClickHouse uses for a
// high-cardinality column.
//
// Object layout, ahead of the ordinary block-framed container:
//
//	[uvarint entryCount][uvarint compressedDictLen][compressed dictionary]
//	[ block-framed container of granule streams ]
//
// The dictionary blob is [uvarint len][bytes] per entry. Each granule stream is prefixed with a mode
// byte: [modeShared] then idWidth-byte big-endian ids, or [modeSelf] then an ordinary chunk bytes
// stream. Mixing the two per granule is what lets one column hold both shapes — a service that logs
// clean structured lines and then dumps a stack trace.
const (
	// modeShared marks a granule encoded as ids into the column's shared dictionary.
	modeShared byte = 0
	// modeSelf marks a granule carrying its own self-describing chunk bytes stream, for values the
	// shared dictionary would not pay for.
	modeSelf byte = 1
)

// sharedDictMinRepeat is the dedup a granule must show to join the shared dictionary: its distinct
// values must be at most this fraction of its rows. A granule at or above it repeats enough that
// entries are worth carrying column-wide; below it, the granule would add roughly one entry per row
// and the dictionary becomes the column with an id array bolted on.
//
// Two rather than a tuned ratio because the decision only has to separate two clearly-separated
// populations — attribute blobs repeating hundreds of times against near-unique message bodies —
// and a threshold in between costs nothing to either.
const sharedDictMinRepeat = 2

// byteAt returns row i of a bytes column in either of its input forms.
func (c Column) byteAt(i int) []byte {
	if c.Bytes != nil {
		return c.Bytes[i]
	}

	return c.BytesBlob[c.BytesOffsets[i]:c.BytesOffsets[i+1]]
}

// encodeSharedDictBytes serializes a bytes column as a shared-dictionary block-framed object. It
// reports ok=false when no granule chose the shared dictionary, leaving the caller on the ordinary
// per-granule path rather than paying an empty dictionary's header for nothing.
func encodeSharedDictBytes(
	c Column, comp *compress.Compressor, blockRows, compressBytes int,
) (obj []byte, ok bool, err error) {
	n := c.rows()
	if n == 0 || blockRows <= 0 {
		return nil, false, nil
	}

	m := pool.NewByteIntMap()
	defer m.PutBack()

	var (
		entries [][]byte
		ids     = make([]int32, n) // global entry id per row; -1 where the granule self-encodes
		shared  = make([]bool, (n+blockRows-1)/blockRows)
		used    bool
	)

	// Pass 1: decide per granule and build the dictionary. The decision needs the granule's distinct
	// count, which is the same probe that assigns its ids, so the ids are kept rather than recomputed.
	seen := pool.NewByteIntMap()
	defer seen.PutBack()

	for g := range shared {
		lo := g * blockRows
		hi := min(lo+blockRows, n)

		distinct := 0

		seen.Reset()

		for i := lo; i < hi; i++ {
			if _, dup := seen.Get(c.byteAt(i)); !dup {
				seen.Put(c.byteAt(i), 1)

				distinct++
			}
		}

		// Near-unique granules stay out: they would grow the shared dictionary by their whole
		// distinct set to serve themselves alone.
		if distinct*sharedDictMinRepeat > hi-lo || len(entries)+distinct > maxSharedEntries {
			for i := lo; i < hi; i++ {
				ids[i] = -1
			}

			continue
		}

		shared[g], used = true, true

		for i := lo; i < hi; i++ {
			v := c.byteAt(i)

			id, hit := m.Get(v)
			if !hit {
				id = len(entries)
				m.Put(v, id)
				entries = append(entries, v)
			}

			ids[i] = int32(id)
		}
	}

	if !used {
		return nil, false, nil
	}

	idWidth := 1
	if len(entries) > 256 {
		idWidth = 2
	}

	body, err := encodeBlockedWith(n, comp, blockRows, compressBytes,
		func(dst []byte, lo, hi int) ([]byte, error) {
			g := lo / blockRows
			if !shared[g] {
				dst = append(dst, modeSelf)

				return appendBlockStream(dst, c, chunk.CodecDict, 0, lo, hi)
			}

			dst = append(dst, modeShared)

			for i := lo; i < hi; i++ {
				if idWidth == 1 {
					dst = append(dst, byte(ids[i]))
					continue
				}

				dst = append(dst, byte(uint16(ids[i])>>8), byte(uint16(ids[i])))
			}

			return dst, nil
		})
	if err != nil {
		return nil, false, err
	}

	var dict []byte
	for _, e := range entries {
		dict = binary.AppendUvarint(dict, uint64(len(e)))
		dict = append(dict, e...)
	}

	packed := comp.Compress(nil, dict)

	obj = binary.AppendUvarint(nil, uint64(len(entries)))
	obj = binary.AppendUvarint(obj, uint64(len(packed)))
	obj = append(obj, packed...)

	return append(obj, body...), true, nil
}

// maxSharedEntries is the largest shared dictionary a 2-byte id can address. A column needing more
// keeps its later granules on the self-encoded path rather than rolling over to a second dictionary:
// a column that has already produced 65536 distinct values is one whose repeats have stopped paying,
// which is the case the self-encoded path exists for.
const maxSharedEntries = 1 << 16

// parseSharedDict peels the dictionary header off a shared-dictionary column object, returning the
// decoded entries and the ordinary block-framed container that follows.
func parseSharedDict(object []byte, comp *compress.Compressor) (entries [][]byte, rest []byte, err error) {
	count, n := binary.Uvarint(object)
	if n <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "shared dict: bad entry count")
	}

	object = object[n:]

	packedLen, n := binary.Uvarint(object)
	if n <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "shared dict: bad dictionary length")
	}

	object = object[n:]

	if packedLen > uint64(len(object)) {
		return nil, nil, errors.Wrapf(ErrCorrupt, "shared dict: length %d past object %d", packedLen, len(object))
	}

	dict, err := comp.Decompress(nil, object[:packedLen])
	if err != nil {
		return nil, nil, errors.Wrap(err, "decompress shared dictionary")
	}

	if count > uint64(len(dict))+1 {
		return nil, nil, errors.Wrapf(ErrCorrupt, "shared dict: %d entries in %d bytes", count, len(dict))
	}

	entries = make([][]byte, 0, count)

	for range count {
		l, n := binary.Uvarint(dict)
		if n <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "shared dict: bad entry length")
		}

		dict = dict[n:]

		if l > uint64(len(dict)) {
			return nil, nil, errors.Wrapf(ErrCorrupt, "shared dict: entry %d past dictionary", len(entries))
		}

		entries = append(entries, dict[:l])
		dict = dict[l:]
	}

	return entries, object[packedLen:], nil
}

// decodeSharedGranule decodes one granule of a shared-dictionary column into dc, resolving shared
// ids against entries. rows is the granule's row count.
func decodeSharedGranule(stream []byte, entries [][]byte, rows int, dc *chunk.DictColumn) error {
	if len(stream) == 0 {
		return errors.Wrap(ErrCorrupt, "shared dict: empty granule stream")
	}

	mode, payload := stream[0], stream[1:]

	if mode == modeSelf {
		if _, err := dc.DecodeBytes(payload); err != nil {
			return err
		}

		return nil
	}

	if mode != modeShared {
		return errors.Wrapf(ErrCorrupt, "shared dict: unknown granule mode %d", mode)
	}

	idWidth := 1
	if len(entries) > 256 {
		idWidth = 2
	}

	if len(payload) != rows*idWidth {
		return errors.Wrapf(ErrCorrupt, "shared dict: %d id bytes for %d rows at width %d",
			len(payload), rows, idWidth)
	}

	for i := range rows {
		id := int(payload[i])
		if idWidth == 2 {
			id = int(payload[i*2])<<8 | int(payload[i*2+1])
		}

		if id >= len(entries) {
			return errors.Wrapf(ErrCorrupt, "shared dict: id %d past %d entries", id, len(entries))
		}
	}

	// The ids index the column-wide dictionary directly, so the granule needs no remap: hand the
	// shared entries straight through as the granule's own.
	dc.Entries, dc.IDs, dc.IDWidth = entries, payload, idWidth

	return nil
}

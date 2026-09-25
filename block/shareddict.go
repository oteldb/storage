package block

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// Shared-dictionary bytes columns carry one dictionary for the whole column, each granule holding
// ids into it or opting out and self-encoding (see ARCH.md). Two layouts exist:
//
//	leading (read only):  [dictionary region][block-framed container]
//	trailer (written):    [frames][dictionary region][directory][u32le dirLen]
//
// The dictionary region is the same in both:
//
//	[uvarint entryCount][uvarint compressedDictLen][compressed dictionary][u32le CRC32C]
//
// its checksum present only when the column is checked ([ColumnDesc.Checked]). The dictionary blob
// is [uvarint len][bytes] per entry. Each granule stream opens with a mode byte; which modes are
// valid depends on the layout ([sharedDict.granule]).
const (
	// modeShared marks a leading-layout granule of ids into the dictionary, at the width the final
	// dictionary size implies.
	modeShared byte = 0
	// modeSelf marks a granule carrying its own chunk bytes stream.
	modeSelf byte = 1
)

// sharedDictMinRepeat is the dedup a granule must show to join the shared dictionary: its distinct
// values must be at most this fraction of its rows. Two separates the two populations that matter —
// attribute blobs repeating hundreds of times against near-unique bodies — at no cost to either.
const sharedDictMinRepeat = 2

// byteAt returns row i of a bytes column in any of its input forms.
func (c Column) byteAt(i int) []byte {
	if c.Bytes != nil {
		return c.Bytes[i]
	}

	if len(c.BytesOffsets) > 0 {
		return c.BytesBlob[c.BytesOffsets[i]:c.BytesOffsets[i+1]]
	}

	return c.BytesDict[c.BytesIDs[i]]
}

// sharedDictJoins is the granule decision before the byte cap: whether a granule of the given row
// and distinct-value counts joins a dictionary already holding entries values.
func sharedDictJoins(distinct, rows, entries int) bool {
	return distinct*sharedDictMinRepeat <= rows && entries+distinct <= maxSharedEntries
}

// maxSharedEntries is the largest shared dictionary a 2-byte id can address. A column needing more
// keeps its later granules self-encoded rather than rolling over to a second dictionary.
//
// A var so a test can lower it instead of building the 130k-row column it otherwise takes.
var maxSharedEntries = sharedEntriesCeiling

// sharedDict is a column's parsed shared dictionary and the granule grammar its layout implies.
type sharedDict struct {
	entries [][]byte
	on      bool
	trailer bool
}

// granule splits a granule stream into its ids, one per row, and their width, or reports self with
// the chunk stream that follows the mode byte. Ids are bounds-checked here: they outlive the decode
// inside the returned column, where an unchecked one would panic at [chunk.DictColumn.At].
func (sd sharedDict) granule(stream []byte, rows int) (ids []byte, width int, self bool, err error) {
	if len(stream) == 0 {
		return nil, 0, false, errors.Wrap(ErrCorrupt, "shared dict: empty granule stream")
	}

	mode, payload := stream[0], stream[1:]

	switch {
	case mode == modeSelf:
		return payload, 0, true, nil
	case !sd.trailer && mode == modeShared:
		width = sharedIDWidth(sd.entries)
	case sd.trailer && mode == modeSharedNarrow:
		width = 1
	case sd.trailer && mode == modeSharedWide && len(sd.entries) > 256:
		width = 2
	default:
		return nil, 0, false, errors.Wrapf(ErrCorrupt,
			"shared dict: granule mode %d invalid for %d entries (trailer %v)", mode, len(sd.entries), sd.trailer)
	}

	if len(payload) != rows*width {
		return nil, 0, false, errors.Wrapf(ErrCorrupt,
			"shared dict: %d id bytes for %d rows at width %d", len(payload), rows, width)
	}

	if err := boundSharedIDs(payload, width, rows, sd.entries); err != nil {
		return nil, 0, false, err
	}

	return payload, width, false, nil
}

// dictLimit bounds a dictionary's decompression: an exact size, or an upper bound, or none when
// limit is negative.
type dictLimit struct {
	limit int64
	exact bool
}

// parseSharedDict parses a dictionary region at the head of object, returning the entries and the
// bytes after the region.
func parseSharedDict(
	object []byte, comp *compress.Compressor, checked bool, lim dictLimit,
) (entries [][]byte, rest []byte, err error) {
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

	packed := object[:packedLen]
	rest = object[packedLen:]

	if checked {
		if len(rest) < objectCRCBytes {
			return nil, nil, errors.Wrap(ErrCorrupt, "shared dict: truncated before its checksum")
		}

		want := binary.LittleEndian.Uint32(rest)
		if got := crc32.Checksum(packed, castagnoli); got != want {
			return nil, nil, errors.Wrapf(ErrCorrupt,
				"shared dict: checksum %08x, want %08x", got, want)
		}

		rest = rest[objectCRCBytes:]
	}

	dict, err := decompressBounded(comp, nil, packed, lim.limit, lim.exact)
	if err != nil {
		return nil, nil, errors.Wrap(err, "shared dictionary")
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

	return entries, rest, nil
}

// parseTrailerDict parses a trailer column's dictionary region, which must be exactly what the
// manifest describes.
func parseTrailerDict(region []byte, comp *compress.Compressor, desc ColumnDesc) (sharedDict, error) {
	entries, rest, err := parseSharedDict(region, comp, desc.Checked, dictLimit{limit: desc.DictRaw, exact: true})
	if err != nil {
		return sharedDict{}, err
	}

	if len(rest) != 0 {
		return sharedDict{}, errors.Wrapf(ErrCorrupt, "shared dict: %d bytes past the region", len(rest))
	}

	if int64(len(entries)) != desc.DictEntries {
		return sharedDict{}, errors.Wrapf(ErrCorrupt,
			"shared dict: %d entries, the manifest says %d", len(entries), desc.DictEntries)
	}

	return sharedDict{entries: entries, on: true, trailer: true}, nil
}

// sharedIDWidth is the bytes per row a leading-layout granule spends on ids into a dictionary of
// the given size.
func sharedIDWidth(entries [][]byte) int {
	if len(entries) > 256 {
		return 2
	}

	return 1
}

// sharedIDAt reads row r out of a granule's packed big-endian id array.
func sharedIDAt(ids []byte, idWidth, r int) int {
	if idWidth == 1 {
		return int(ids[r])
	}

	return int(uint16(ids[r*2])<<8 | uint16(ids[r*2+1]))
}

// boundSharedIDs rejects a granule whose packed ids do not all index entries.
func boundSharedIDs(ids []byte, idWidth, rows int, entries [][]byte) error {
	for r := range rows {
		if id := sharedIDAt(ids, idWidth, r); id >= len(entries) {
			return errors.Wrapf(ErrCorrupt, "shared dict: id %d past %d entries", id, len(entries))
		}
	}

	return nil
}

// decodeSharedIDs is the fast path for a shared-dictionary column whose selected granules all use
// the dictionary: the result is the dictionary plus the granules' ids copied into place, at the
// width the whole dictionary needs — a narrow trailer granule is widened — so it equals what the
// leading layout decodes to. It reports ok=false at the first self-encoded granule, leaving the
// caller to redo the walk through [chunk.DictMerger].
//
// Ids are copied, so one decompression buffer serves every frame. In scatter mode rows no granule
// covers are left at id 0, i.e. unspecified.
func decodeSharedIDs(
	dir blockDir, comp *compress.Compressor, rows int, blocks []int, sd sharedDict, scatter bool,
) (col *chunk.DictColumn, ok bool, err error) {
	idWidth := sharedIDWidth(sd.entries)

	out := rows
	if !scatter {
		out = 0
		for _, g := range blocks {
			lo := g * dir.blockRows
			if lo >= rows {
				return nil, false, errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", g, lo, rows)
			}

			out += min(lo+dir.blockRows, rows) - lo
		}
	}

	ids := make([]byte, out*idWidth)
	streams := newWalkStreams(dir, comp)
	defer streams.release()
	pos := 0

	for _, g := range blocks {
		if g < 0 || g >= dir.nBlocks() {
			return nil, false, errors.Errorf("block: block %d out of range [0,%d)", g, dir.nBlocks())
		}

		lo := g * dir.blockRows
		if lo >= rows {
			return nil, false, errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", g, lo, rows)
		}

		stream, err := streams.granule(g)
		if err != nil {
			return nil, false, err
		}

		n := min(lo+dir.blockRows, rows) - lo

		gids, width, self, err := sd.granule(stream, n)
		if err != nil {
			return nil, false, err
		}

		if self {
			return nil, false, nil
		}

		at := pos
		if scatter {
			at = lo
		}

		dst := ids[at*idWidth : (at+n)*idWidth]
		if width == idWidth {
			copy(dst, gids)
		} else {
			for r, id := range gids {
				dst[2*r+1] = id
			}
		}

		pos += n
	}

	return &chunk.DictColumn{Entries: sd.entries, IDs: ids, IDWidth: idWidth}, true, nil
}

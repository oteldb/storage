package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

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
//	[uvarint entryCount][uvarint compressedDictLen][compressed dictionary][u32le CRC32C]
//	[ block-framed container of granule streams ]
//
// The checksum covers the compressed dictionary and is present only when the manifest says the
// column is checked ([ColumnDesc.Checked]); the granule streams are covered by the per-frame
// checksums in the block-framed container's directory.
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

// sharedDictJoins applies the granule decision: whether a granule of the given row and distinct-value
// counts joins a column-wide dictionary already holding entries values. Near-unique granules stay out
// — they would grow the shared dictionary by their whole distinct set to serve themselves alone.
//
// The two input forms reach it with the same numbers (entries are distinct by value, so counting
// distinct indices counts distinct values), which is what keeps their objects byte-identical.
func sharedDictJoins(distinct, rows, entries int) bool {
	return distinct*sharedDictMinRepeat <= rows && entries+distinct <= maxSharedEntries
}

// buildSharedDictFromValues decides each granule and builds the column dictionary for a column whose
// cells are flat values, hashing each row twice: once to count the granule's distinct values, once to
// assign its shared-dictionary id. It fills shared (per granule) and ids (per row, -1 where the
// granule self-encodes), and reports whether any granule joined.
func buildSharedDictFromValues(c Column, blockRows int, shared []bool, ids []int32) ([][]byte, bool) {
	n := len(ids)

	m := pool.NewByteIntMap()
	defer m.PutBack()

	seen := pool.NewByteIntMap()
	defer seen.PutBack()

	var (
		entries [][]byte
		used    bool
	)

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

		if !sharedDictJoins(distinct, hi-lo, len(entries)) {
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

	return entries, used
}

// buildSharedDictFromIDs is [buildSharedDictFromValues] for a column already in split form. Both
// passes become array work over int32s: a per-source-entry stamp counts a granule's distinct ids
// without clearing between granules, and remap carries a source entry's shared-dictionary id once
// assigned. No value is hashed or compared.
func buildSharedDictFromIDs(dict [][]byte, src []int32, blockRows int, shared []bool, ids []int32) ([][]byte, bool) {
	n := len(ids)

	// stamp is freshly zeroed and gen starts at 1, so no generation ever wraps onto a stale stamp:
	// gen advances once per granule and a column cannot hold 2³² of them.
	stamp := make([]uint32, len(dict))
	remap := make([]int32, len(dict))

	for i := range remap {
		remap[i] = -1
	}

	var (
		entries [][]byte
		used    bool
		gen     uint32
	)

	for g := range shared {
		lo := g * blockRows
		hi := min(lo+blockRows, n)

		distinct := 0
		gen++

		for i := lo; i < hi; i++ {
			e := src[i]

			// The shared-dictionary path never reaches the chunk encoder's equivalent guard, so it
			// carries its own, naming the row and the table instead of raising a bare bounds error
			// from inside the stamp array. Compared against len(stamp) rather than the equal
			// len(dict) so the compiler can see it dominates the indexing below.
			if uint32(e) >= uint32(len(stamp)) {
				panic(fmt.Sprintf(
					"block: dictionary id %d at row %d is out of range for %d entries", e, i, len(dict)))
			}

			if stamp[e] != gen {
				stamp[e] = gen

				distinct++
			}
		}

		if !sharedDictJoins(distinct, hi-lo, len(entries)) {
			for i := lo; i < hi; i++ {
				ids[i] = -1
			}

			continue
		}

		shared[g], used = true, true

		for i := lo; i < hi; i++ {
			e := src[i]

			id := remap[e]
			if id < 0 {
				id = int32(len(entries))
				remap[e] = id
				entries = append(entries, dict[e])
			}

			ids[i] = id
		}
	}

	return entries, used
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

	ids := make([]int32, n) // shared-dictionary id per row; -1 where the granule self-encodes
	shared := make([]bool, (n+blockRows-1)/blockRows)

	var (
		entries [][]byte
		used    bool
	)

	if c.bytesSplitForm() {
		entries, used = buildSharedDictFromIDs(c.BytesDict, c.BytesIDs, blockRows, shared, ids)
	} else {
		entries, used = buildSharedDictFromValues(c, blockRows, shared, ids)
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
	obj = binary.LittleEndian.AppendUint32(obj, crc32.Checksum(packed, castagnoli))

	return append(obj, body...), true, nil
}

// maxSharedEntries is the largest shared dictionary a 2-byte id can address. A column needing more
// keeps its later granules on the self-encoded path rather than rolling over to a second dictionary:
// a column that has already produced 65536 distinct values is one whose repeats have stopped paying,
// which is the case the self-encoded path exists for.
//
// A var, not a const, so a test can lower it instead of building the 130k-row column it otherwise
// takes to reach the ceiling (the same reason [byteColCap] is one in recordengine).
var maxSharedEntries = 1 << 16

// parseSharedDict peels the dictionary header off a shared-dictionary column object, returning the
// decoded entries and the ordinary block-framed container that follows.
func parseSharedDict(
	object []byte, comp *compress.Compressor, checked bool,
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

	dict, err := comp.Decompress(nil, packed)
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

	return entries, rest, nil
}

// sharedIDWidth is the bytes per row a granule spends on ids into a dictionary of the given size.
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

// boundSharedIDs rejects a granule whose packed ids do not all index entries. Every path that
// accepts shared-dictionary ids runs it: the ids outlive the decode inside the returned column, so
// an unchecked one surfaces as a panic at [chunk.DictColumn.At] rather than a decode error.
func boundSharedIDs(ids []byte, idWidth, rows int, entries [][]byte) error {
	for r := range rows {
		if id := sharedIDAt(ids, idWidth, r); id >= len(entries) {
			return errors.Wrapf(ErrCorrupt, "shared dict: id %d past %d entries", id, len(entries))
		}
	}

	return nil
}

// splitSharedGranule peels a granule stream's leading mode byte, rejecting a mode this reader does
// not know rather than treating its payload as one of the two it does.
func splitSharedGranule(stream []byte) (mode byte, payload []byte, err error) {
	if len(stream) == 0 {
		return 0, nil, errors.Wrap(ErrCorrupt, "shared dict: empty granule stream")
	}

	switch mode = stream[0]; mode {
	case modeShared, modeSelf:
		return mode, stream[1:], nil
	default:
		return 0, nil, errors.Wrapf(ErrCorrupt, "shared dict: unknown granule mode %d", mode)
	}
}

// decodeSharedIDs is the fast path for a shared-dictionary column whose selected granules all use
// it: their ids already index the column-wide dictionary, so the result is the dictionary plus the
// granules' id bytes copied into place — no per-granule dictionary to merge, no remap, no hash
// probes. This is the common case (measured: the shared dictionary is chosen in 33 of 34 parts for
// a record-attributes column), and it is what keeps a framed decode close to the single-stream one.
//
// It reports ok=false the moment it meets a self-encoded granule, leaving the caller to redo the
// walk through [chunk.DictMerger]; only a column mixing both modes pays that second pass.
//
// Because the ids are *copied* rather than aliased, this can reuse one decompression buffer across
// frames — unlike the merge path, whose entries alias the frames and so must keep each alive.
//
// In scatter mode the rows no granule covers are left at id 0, i.e. unspecified — the same contract
// the int64 path has, where decodeBlocksInto leaves unselected rows at whatever the destination
// held. A caller reads only the rows it selected.
func decodeSharedIDs(
	dir blockDir, comp *compress.Compressor, rows int, blocks []int, entries [][]byte, scatter bool,
) (col *chunk.DictColumn, ok bool, err error) {
	idWidth := 1
	if len(entries) > 256 {
		idWidth = 2
	}

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
	streams := newBlockStreams(dir, comp)
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

		if len(stream) == 0 {
			return nil, false, errors.Wrap(ErrCorrupt, "shared dict: empty granule stream")
		}

		if stream[0] != modeShared {
			return nil, false, nil // a self-encoded granule: the caller redoes this through the merge
		}

		n := min(lo+dir.blockRows, rows) - lo
		if len(stream[1:]) != n*idWidth {
			return nil, false, errors.Wrapf(ErrCorrupt,
				"shared dict: %d id bytes for %d rows at width %d", len(stream[1:]), n, idWidth)
		}

		// Bounds-checked here as the merge path does in appendShared: an unchecked id survives into
		// the returned column and panics later at chunk.DictColumn.At, inside the caller's row loop
		// with nothing naming the granule it came from.
		if err := boundSharedIDs(stream[1:], idWidth, n, entries); err != nil {
			return nil, false, err
		}

		at := pos
		if scatter {
			at = lo
		}

		copy(ids[at*idWidth:], stream[1:])
		pos += n
	}

	return &chunk.DictColumn{Entries: entries, IDs: ids, IDWidth: idWidth}, true, nil
}

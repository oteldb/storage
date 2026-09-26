package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/pool"
)

const (
	// dictEntryBytes is what a writer's dictionary entry holds besides its value: a slice header, a
	// count, a granule-to-dictionary slot and its share of the index. It is charged per entry
	// against the byte cap, so the cap bounds the dictionary's resident size and not only its bytes.
	dictEntryBytes = 147
	// defaultSharedDictBytes is the default byte cap ([WithSharedDictBytes]), above the largest
	// dictionary measured on real data (17.8 MiB).
	defaultSharedDictBytes = 32 << 20
)

// Granule modes of a trailer-dictionary column. The width a granule's ids take is fixed when it is
// encoded, so a granule written while the dictionary still fits one byte stays narrow.
const (
	modeSharedNarrow byte = 2
	modeSharedWide   byte = 3
)

// sharedDictBuilder decides granule by granule whether a granule joins the column's dictionary D,
// growing D as granules join. The decision is [sharedDictJoins] plus a byte cap charged only for the
// values a granule would add, so the cap changes an outcome only when D would really outgrow it.
//
// Entries alias the input column, which outlives the build.
type sharedDictBuilder struct {
	cap     int64
	charge  int64
	raw     int64
	entries [][]byte

	// The values path: D's index, and a granule's distinct values with their entry (-1 when new).
	index     *pool.ByteIntMap
	seen      *pool.ByteIntMap
	locals    []int32
	localVals [][]byte
	rowLocal  []int32

	// The ids path: a source entry's D id (-1 until it joins), a per-granule stamp counting distinct
	// source entries without a clear, and the granule's distinct source entries in row order.
	remap  []int32
	stamp  []uint32
	gen    uint32
	firsts []int32

	// Kept only for a [BytesObserver]: rows per D entry, and per distinct value of the granule just
	// decided (slot maps a source entry to its place in firsts).
	observe bool
	counts  []uint64
	gCounts []uint64
	gVals   [][]byte
	slot    []int32
}

func newSharedDictBuilder(dictCap int64, observe bool) *sharedDictBuilder {
	return &sharedDictBuilder{cap: dictCap, observe: observe}
}

func (b *sharedDictBuilder) release() {
	if b.index != nil {
		b.index.PutBack()
		b.seen.PutBack()
		b.index, b.seen = nil, nil
	}
}

func entryCharge(v []byte) (raw, charge int64) {
	raw = int64(varintLen(uint64(len(v))) + len(v))

	return raw, raw + dictEntryBytes
}

func (b *sharedDictBuilder) joins(distinct, rows int, newCharge int64) bool {
	return sharedDictJoins(distinct, rows, len(b.entries)) && newCharge <= b.cap-b.charge
}

func (b *sharedDictBuilder) insert(v []byte) int32 {
	raw, charge := entryCharge(v)
	b.raw += raw
	b.charge += charge
	b.entries = append(b.entries, v)

	if b.observe {
		b.counts = append(b.counts, 0)
	}

	return int32(len(b.entries) - 1)
}

// zeroCounts resets gCounts to distinct zeroed slots, one per distinct value of the granule.
func (b *sharedDictBuilder) zeroCounts(distinct int) {
	b.gCounts = slices.Grow(b.gCounts[:0], distinct)[:distinct]
	clear(b.gCounts)
}

// region serializes D as a dictionary region, returning it and the blob length it packs.
func (b *sharedDictBuilder) region(comp *compress.Compressor) ([]byte, int64) {
	blob := make([]byte, 0, b.raw)
	for _, e := range b.entries {
		blob = binary.AppendUvarint(blob, uint64(len(e)))
		blob = append(blob, e...)
	}

	return dictRegion(comp, blob, len(b.entries)), int64(len(blob))
}

// width is the id width a granule joining now is encoded at.
func (b *sharedDictBuilder) width() int {
	if len(b.entries) > 256 {
		return 2
	}

	return 1
}

// addValues decides rows [lo,hi) of a column in flat form, filling ids with their D ids when the
// granule joins. It hashes each row once and each distinct value of the granule once more.
func (b *sharedDictBuilder) addValues(c Column, lo, hi int, ids []int32) bool {
	if b.index == nil {
		b.index, b.seen = pool.NewByteIntMap(), pool.NewByteIntMap()
	}

	b.seen.Reset()
	b.locals, b.localVals, b.rowLocal = b.locals[:0], b.localVals[:0], b.rowLocal[:0]

	var newCharge int64

	for i := lo; i < hi; i++ {
		v := c.byteAt(i)

		local, dup := b.seen.PutOrGet(v, len(b.locals))
		if !dup {
			did, in := b.index.Get(v)
			if !in {
				did = -1
				_, charge := entryCharge(v)
				newCharge += charge
			}

			b.locals = append(b.locals, int32(did))
			b.localVals = append(b.localVals, v)
		}

		b.rowLocal = append(b.rowLocal, int32(local))
	}

	if b.observe {
		b.zeroCounts(len(b.locals))

		for _, l := range b.rowLocal {
			b.gCounts[l]++
		}

		b.gVals = b.localVals
	}

	if !b.joins(len(b.locals), hi-lo, newCharge) {
		return false
	}

	for l, did := range b.locals {
		if did < 0 {
			b.locals[l] = b.insert(b.localVals[l])
			b.index.Put(b.localVals[l], int(b.locals[l]))
		}

		if b.observe {
			b.counts[b.locals[l]] += b.gCounts[l]
		}
	}

	for i, l := range b.rowLocal {
		ids[i] = b.locals[l]
	}

	return true
}

// addIDs is [sharedDictBuilder.addValues] over a column in split form: array work over entry
// indices, hashing nothing.
func (b *sharedDictBuilder) addIDs(dict [][]byte, src []int32, lo, hi int, ids []int32) bool {
	if b.remap == nil {
		// stamp is freshly zeroed and gen starts at 1, so no generation wraps onto a stale stamp.
		b.stamp = make([]uint32, len(dict))
		b.remap = make([]int32, len(dict))

		if b.observe {
			b.slot = make([]int32, len(dict))
		}

		for i := range b.remap {
			b.remap[i] = -1
		}
	}

	b.gen++
	b.firsts = b.firsts[:0]

	var newCharge int64

	for i := lo; i < hi; i++ {
		e := src[i]

		// The chunk encoder's equivalent guard is never reached on this path, so it carries its
		// own, naming the row instead of raising a bare bounds error from inside the stamp array.
		if uint32(e) >= uint32(len(b.stamp)) {
			panic(fmt.Sprintf(
				"block: dictionary id %d at row %d is out of range for %d entries", e, i, len(dict)))
		}

		if b.stamp[e] != b.gen {
			b.stamp[e] = b.gen

			if b.observe {
				b.slot[e] = int32(len(b.firsts))
			}

			b.firsts = append(b.firsts, e)

			if b.remap[e] < 0 {
				_, charge := entryCharge(dict[e])
				newCharge += charge
			}
		}
	}

	if b.observe {
		b.zeroCounts(len(b.firsts))

		for _, e := range src[lo:hi] {
			b.gCounts[b.slot[e]]++
		}

		b.gVals = b.gVals[:0]
		for _, e := range b.firsts {
			b.gVals = append(b.gVals, dict[e])
		}
	}

	if !b.joins(len(b.firsts), hi-lo, newCharge) {
		return false
	}

	for k, e := range b.firsts {
		if b.remap[e] < 0 {
			b.remap[e] = b.insert(dict[e])
		}

		if b.observe {
			b.counts[b.remap[e]] += b.gCounts[k]
		}
	}

	for i := lo; i < hi; i++ {
		ids[i-lo] = b.remap[src[i]]
	}

	return true
}

// trailerDict describes a trailer dictionary region as the manifest records it.
type trailerDict struct {
	off, length, raw, entries int64
}

func (t trailerDict) apply(desc *ColumnDesc) {
	desc.Blocked, desc.Framed, desc.SharedDict, desc.Footer, desc.TrailerDict = true, true, true, true, true
	desc.DictOff, desc.DictLen, desc.DictRaw, desc.DictEntries = t.off, t.length, t.raw, t.entries
}

// encodeTrailerDictBytes serializes a bytes column under the trailer-dictionary layout:
//
//	[frames][dictionary region][directory][u32le dirLen]
//
// The region is byte-identical to the leading layout's header, and each granule stream opens with
// its mode byte: [modeSelf] then a chunk bytes stream, or [modeSharedNarrow]/[modeSharedWide] then
// 1- or 2-byte big-endian ids. It reports ok=false when no granule joins, unless framed forces the
// layout regardless. The column's observer hears every declined granule, and the dictionary only
// when ok.
func encodeTrailerDictBytes(
	c Column, comp *compress.Compressor, l columnLayout, framed bool,
) (obj []byte, dict trailerDict, sizing ColumnSizing, ok bool, err error) {
	n := c.rows()
	if l.blockRows <= 0 || (n == 0 && !framed) {
		return nil, trailerDict{}, ColumnSizing{}, false, nil
	}

	ids := make([]int32, n)
	modes := make([]byte, (n+l.blockRows-1)/l.blockRows)
	used := false

	b := newSharedDictBuilder(l.dictCap, c.Observer != nil)
	defer b.release()

	for g := range modes {
		lo := g * l.blockRows
		hi := min(lo+l.blockRows, n)

		var joined bool
		if c.bytesSplitForm() {
			joined = b.addIDs(c.BytesDict, c.BytesIDs, lo, hi, ids[lo:hi])
		} else {
			joined = b.addValues(c, lo, hi, ids[lo:hi])
		}

		switch {
		case !joined:
			modes[g] = modeSelf

			if c.Observer != nil {
				c.Observer.SelfGranule(b.gVals, b.gCounts)
			}
		case b.width() == 1:
			modes[g], used = modeSharedNarrow, true
		default:
			modes[g], used = modeSharedWide, true
		}
	}

	if !used && !framed {
		return nil, trailerDict{}, ColumnSizing{}, false, nil
	}

	acc, err := accumulateBlocked(n, comp, l.blockRows, l.compressBytes,
		func(dst []byte, lo, hi int) ([]byte, error) {
			mode := modes[lo/l.blockRows]
			if mode == modeSelf {
				return appendBlockStream(append(dst, mode), c, chunk.CodecDict, 0, lo, hi)
			}

			return appendSharedIDs(dst, mode, ids[lo:hi]), nil
		})
	if err != nil {
		return nil, trailerDict{}, ColumnSizing{}, false, err
	}

	if c.Observer != nil {
		c.Observer.Dictionary(b.entries, b.counts)
	}

	region, raw := b.region(comp)
	dict = trailerDict{off: int64(acc.bytes), length: int64(len(region)), raw: raw, entries: int64(len(b.entries))}

	obj = acc.finishBuffered(l.blockRows, region)

	return obj, dict, acc.sizing(), true, nil
}

// appendSharedIDs appends a joined granule: its mode byte, then each row's D id at the mode's width.
func appendSharedIDs(dst []byte, mode byte, ids []int32) []byte {
	dst = append(dst, mode)

	if mode == modeSharedNarrow {
		for _, id := range ids {
			dst = append(dst, byte(id))
		}

		return dst
	}

	for _, id := range ids {
		dst = binary.BigEndian.AppendUint16(dst, uint16(id))
	}

	return dst
}

// dictRegion is a shared dictionary's region:
//
//	[uvarint entryCount][uvarint packedLen][packed][u32le CRC32C(packed)]
func dictRegion(comp *compress.Compressor, blob []byte, entries int) []byte {
	packed := comp.Compress(nil, blob)

	dst := binary.AppendUvarint(nil, uint64(entries))
	dst = binary.AppendUvarint(dst, uint64(len(packed)))
	dst = append(dst, packed...)

	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(packed, castagnoli))
}

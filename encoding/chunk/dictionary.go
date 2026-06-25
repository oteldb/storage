package chunk

import (
	"slices"
	"sync"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/pool"
)

// maxDictSize is the maximum dictionary cardinality for the 1-byte-id fast path.
// Values above this fall back to a 2-byte id. Matches VictoriaLogs/Parquet dict
// thresholds (DESIGN.md §6: "≤256 distinct → 1 byte/row").
const maxDictSize = 256

// Dictionary format flag bytes (full byte, not a single bit, so all subsequent bulk
// writes stay byte-aligned for the fast [bitstream.Writer.AppendString]/
// [bitstream.Writer.WriteBytes] paths).
const (
	flagFlat byte = 0x00 // no dictionary, values stored inline
	flagDict byte = 0x01 // dictionary mode
)

var dictEncodeScratchPool = sync.Pool{
	New: func() any { return &dictEncodeScratch{} },
}

type dictEncodeScratch struct {
	entries [][]byte
	ids     []uint16
}

var dictDecodeScratchPool = sync.Pool{
	New: func() any { return &dictDecodeScratch{} },
}

type dictDecodeScratch struct {
	entries [][]byte
}

func newDictEncodeScratch(rows int) *dictEncodeScratch {
	s := dictEncodeScratchPool.Get().(*dictEncodeScratch)
	s.entries = s.entries[:0]
	s.ids = slices.Grow(s.ids[:0], rows)[:rows]

	return s
}

func (s *dictEncodeScratch) putBack() {
	clear(s.entries)
	s.entries = s.entries[:0]
	s.ids = s.ids[:0]
	dictEncodeScratchPool.Put(s)
}

func newDictDecodeScratch(entries int) *dictDecodeScratch {
	s := dictDecodeScratchPool.Get().(*dictDecodeScratch)
	s.entries = slices.Grow(s.entries[:0], entries)[:entries]

	return s
}

func (s *dictDecodeScratch) putBack() {
	clear(s.entries)
	s.entries = s.entries[:0]
	dictDecodeScratchPool.Put(s)
}

// EncodeBytes appends a dictionary-encoded []byte column to dst and returns the
// extended slice (DESIGN.md §6, §14 M0).
//
// It uses a [pool.ByteIntMap] (xxh3-based open-addressing hash table) instead of
// Go's map[string]int: xxh3 is faster than the runtime's string hash, and []byte
// keys avoid the string-conversion allocation on every lookup. For low-cardinality
// columns (≤256 distinct), each row id is 1 byte.
//
// Layout: [uvarint rows] [1 byte flag] [payload]
//
// flagDict payload: [uvarint dictSize] [dict entries: per entry uvarint len + bytes]
// [row ids: 1 byte each if dictSize ≤ 256, else 2 bytes big-endian each]
//
// flagFlat payload: per row [uvarint len][bytes] (the flat fallback for >65536 distinct).
//
// For repeated encodes of many column batches, prefer a [DictEncoder]: it keeps the
// hash table and scratch slices cache-warm across calls instead of paying the
// [sync.Pool] Get/Put on each call.
func EncodeBytes(dst []byte, vals [][]byte) []byte {
	if len(vals) == 0 {
		return appendEmpty(dst)
	}

	// Single-pass dictionary build with [pool.ByteIntMap]: xxh3 hash + []byte keys,
	// one probe per value, with row ids and dictionary entry storage reused via a
	// same-lifetime scratch arena.
	m := pool.NewByteIntMap()
	defer m.PutBack()

	scratch := newDictEncodeScratch(len(vals))
	defer scratch.putBack()

	dst, scratch.entries, scratch.ids = appendDictEncoded(dst, vals, m, scratch.entries, scratch.ids)

	return dst
}

// appendEmpty writes the canonical empty-column payload: [uvarint 0] then byte pad.
func appendEmpty(dst []byte) []byte {
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(0)
	w.PadToByte()

	return w.Bytes()
}

// appendDictEncoded is the shared encode core for [EncodeBytes] and
// [DictEncoder.Encode]. It builds the dictionary into m, reusing the entries and ids
// slices (grown as needed), and appends the encoded column to dst. The grown slices
// are returned so the caller can retain their backing arrays for the next batch.
//
// m is assumed empty (freshly reset); the caller owns its lifecycle.
func appendDictEncoded(
	dst []byte, vals [][]byte, m *pool.ByteIntMap, entries [][]byte, ids []uint16,
) (out []byte, entriesOut [][]byte, idsOut []uint16) {
	if len(vals) == 0 {
		return appendEmpty(dst), entries[:0], ids[:0]
	}

	entries = entries[:0]
	ids = slices.Grow(ids[:0], len(vals))[:len(vals)]

	dictEntryBytes := 0
	flat := false

	for i, v := range vals {
		id, existed := m.PutOrGet(v, len(entries))
		if !existed {
			if len(entries) == 65536 {
				flat = true

				break
			}

			entries = append(entries, v)
			dictEntryBytes += uvarintLen(uint64(len(v))) + len(v)
		}

		ids[i] = uint16(id)
	}

	size := len(entries)

	if flat {
		// Flat fallback: no dictionary, store each value inline.
		// Compute payload size now (deferred from the hot loop above).
		flatPayloadBytes := 0
		for _, v := range vals {
			flatPayloadBytes += uvarintLen(uint64(len(v))) + len(v)
		}

		dst = slices.Grow(dst, uvarintLen(uint64(len(vals)))+1+flatPayloadBytes)
		w := bitstream.NewWriter(dst)
		w.WriteUvarint(uint64(len(vals)))

		_ = w.WriteByte(flagFlat)
		for _, v := range vals {
			w.WriteUvarint(uint64(len(v)))
			w.WriteBytes(v)
		}

		w.PadToByte()

		return w.Bytes(), entries, ids
	}

	// Dictionary mode. Flag is a full byte so everything after stays byte-aligned.
	rowIDBytes := len(vals)
	if size > maxDictSize {
		rowIDBytes *= 2
	}

	dst = slices.Grow(
		dst,
		uvarintLen(uint64(len(vals)))+1+uvarintLen(uint64(size))+dictEntryBytes+rowIDBytes,
	)
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(len(vals)))
	_ = w.WriteByte(flagDict)
	w.WriteUvarint(uint64(size))
	// Write dictionary entries: length-prefixed (uvarint) + bytes (bulk append).
	for _, e := range entries {
		w.WriteUvarint(uint64(len(e)))
		w.WriteBytes(e)
	}
	// Write row ids directly into the writer buffer (byte-aligned here).
	if size <= maxDictSize {
		idBytes := w.AppendBytes(len(ids))
		for i, id := range ids {
			idBytes[i] = byte(id)
		}
	} else {
		idBytes := w.AppendBytes(len(ids) * 2)
		for i, id := range ids {
			idBytes[i*2] = byte(id >> 8)
			idBytes[i*2+1] = byte(id)
		}
	}

	w.PadToByte()

	return w.Bytes(), entries, ids
}

// DictEncoder is a reusable dictionary-column encoder. Unlike the standalone
// [EncodeBytes], it owns its [pool.ByteIntMap] and scratch slices for the lifetime of
// the encoder, so a sequence of [DictEncoder.Encode] calls keeps the hash table and id
// arrays cache-warm and skips the per-call [sync.Pool] Get/Put.
//
// It is intended for the write path, where one goroutine encodes many column batches
// in a row. The zero value is not usable; create one with [NewDictEncoder]. Not safe
// for concurrent use; pool one per goroutine.
type DictEncoder struct {
	m       *pool.ByteIntMap
	entries [][]byte
	ids     []uint16
}

// NewDictEncoder returns a [DictEncoder] with a warm, empty hash table.
func NewDictEncoder() *DictEncoder {
	return &DictEncoder{m: pool.NewByteIntMap()}
}

// Encode appends a dictionary-encoded column for vals to dst and returns the extended
// slice. The output is byte-identical to [EncodeBytes] for the same input. Each call
// resets the encoder's internal state first, so Encode is self-contained.
func (e *DictEncoder) Encode(dst []byte, vals [][]byte) []byte {
	e.m.Reset()

	dst, e.entries, e.ids = appendDictEncoded(dst, vals, e.m, e.entries, e.ids)

	return dst
}

// Reset clears the encoder's retained state without freeing its backing arrays, so the
// next [DictEncoder.Encode] starts from a clean, cache-warm encoder. Encode resets
// implicitly, so calling Reset is only needed to drop references to a prior batch's
// input values held in the entries slice.
func (e *DictEncoder) Reset() {
	e.m.Reset()
	clear(e.entries)
	e.entries = e.entries[:0]
	e.ids = e.ids[:0]
}

// Release returns the encoder's hash table to the shared pool. After Release the
// encoder must not be used. Use it when an encoder's lifetime ends so the map can be
// reused by [NewByteIntMap]/[EncodeBytes].
func (e *DictEncoder) Release() {
	if e.m != nil {
		e.m.PutBack()
		e.m = nil
	}

	e.entries = nil
	e.ids = nil
}

func uvarintLen(u uint64) int {
	n := 1

	for u >= 0x80 {
		u >>= 7
		n++
	}

	return n
}

// DecodeBytes decodes a dictionary-encoded column from src into dst. Returned byte
// slices alias src.
func DecodeBytes(dst [][]byte, src []byte) ([][]byte, int, error) {
	var r bitstream.Reader

	rows, consumed, err := readHeaderInto(src, &r)
	if err != nil {
		return dst, 0, err
	}

	if rows == 0 {
		return dst, consumed, nil
	}

	if cap(dst) < rows {
		dst = resize(dst, rows)
	}

	dst = dst[:rows]

	flag, err := r.ReadByte()
	if err != nil {
		return dst, 0, err
	}

	if flag == flagFlat {
		// Flat fallback.
		for i := range rows {
			ln, err := r.ReadUvarint()
			if err != nil {
				return dst, 0, err
			}

			buf, err := r.ReadBytesView(int(ln))
			if err != nil {
				return dst, 0, err
			}

			dst[i] = buf
		}

		return dst, consumed + r.ConsumedBytes(), nil
	}

	// Dictionary mode.
	dictSize, err := r.ReadUvarint()
	if err != nil {
		return dst, 0, err
	}

	scratch := newDictDecodeScratch(int(dictSize))
	defer scratch.putBack()

	entries := scratch.entries

	for i := range dictSize {
		ln, err := r.ReadUvarint()
		if err != nil {
			return dst, 0, err
		}

		buf, err := r.ReadBytesView(int(ln))
		if err != nil {
			return dst, 0, err
		}

		entries[i] = buf
	}

	if dictSize <= maxDictSize {
		idBuf, err := r.ReadBytesView(rows)
		if err != nil {
			return dst, 0, err
		}

		for i := range rows {
			dst[i] = entries[idBuf[i]]
		}
	} else {
		idBuf, err := r.ReadBytesView(rows * 2)
		if err != nil {
			return dst, 0, err
		}

		for i := range rows {
			dst[i] = entries[(uint16(idBuf[i*2])<<8)|uint16(idBuf[i*2+1])]
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// DictColumn is a decoded dictionary column in its split form: the unique entries plus
// the raw per-row id array, without the gather that materializes one []byte slice
// header per row. It is the output of [DecodeBytesDict].
//
// Profiling (see git history of the dict codec) showed 82% of [DecodeBytes] time was
// the gather loop `dst[i] = entries[idBuf[i]]`, which is pure store bandwidth and
// cannot be sped up in place. The split form defers that work to [DictColumn.At], so a
// query that filters most rows on their id before projecting pays the gather only for
// surviving rows (DESIGN.md §7 fetch contract: decode only what the query touches).
//
// All byte slices alias the source passed to the decode; they are valid only until that
// source is mutated or released. A DictColumn must not be retained past its source.
type DictColumn struct {
	// Entries holds the unique values, indexed by id. In the flat-fallback form
	// (IDWidth == 0) it instead holds one value per row, indexed directly by row.
	Entries [][]byte
	// IDs is the raw row-id array as stored: IDWidth bytes per row, big-endian. It is
	// nil when IDWidth == 0.
	IDs []byte
	// IDWidth is the number of id bytes per row: 1 (dict ≤ 256 entries), 2 (larger
	// dict), or 0 for the flat fallback where Entries is indexed by row directly.
	IDWidth int
}

// Len reports the number of rows in the column. It is derived from the fields so a
// [DictColumn] can be constructed directly (e.g. a constant column: one entry, an
// all-zero IDs slice of the desired length, IDWidth 1).
func (c *DictColumn) Len() int {
	switch c.IDWidth {
	case 0: // flat fallback (or empty): Entries holds one value per row
		return len(c.Entries)
	case 1:
		return len(c.IDs)
	default: // 2-byte ids
		return len(c.IDs) / 2
	}
}

// At returns the value at row, as a view aliasing the decode source. row must be in
// [0, Len()).
func (c *DictColumn) At(row int) []byte {
	switch c.IDWidth {
	case 0: // flat fallback: Entries indexed by row directly
		return c.Entries[row]
	case 1:
		return c.Entries[c.IDs[row]]
	default: // 2-byte big-endian id
		return c.Entries[(uint16(c.IDs[row*2])<<8)|uint16(c.IDs[row*2+1])]
	}
}

// DecodeBytesDict decodes a dictionary-encoded column (as written by [EncodeBytes] or
// [DictEncoder.Encode]) into its split form without gathering per-row values. It
// returns the column, the number of source bytes consumed, and any error. The returned
// [DictColumn] and all its byte slices alias src.
//
// This allocates a fresh [DictColumn]; to decode repeatedly with zero allocations,
// reuse one column via [DictColumn.DecodeBytes].
func DecodeBytesDict(src []byte) (*DictColumn, int, error) {
	c := &DictColumn{}

	consumed, err := c.DecodeBytes(src)
	if err != nil {
		return nil, 0, err
	}

	return c, consumed, nil
}

// DecodeBytes decodes a dictionary-encoded column from src into c, reusing c's Entries
// backing array. It returns the number of source bytes consumed and any error. The
// decoded byte slices alias src.
func (c *DictColumn) DecodeBytes(src []byte) (int, error) {
	var r bitstream.Reader

	rows, consumed, err := readHeaderInto(src, &r)
	if err != nil {
		return 0, err
	}

	c.Entries = c.Entries[:0]
	c.IDs = nil
	c.IDWidth = 0

	if rows == 0 {
		return consumed, nil
	}

	flag, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	if flag == flagFlat {
		// Flat fallback: one entry per row, indexed directly (IDWidth stays 0).
		c.Entries = slices.Grow(c.Entries, rows)[:rows]
		for i := range rows {
			ln, err := r.ReadUvarint()
			if err != nil {
				return 0, err
			}

			buf, err := r.ReadBytesView(int(ln))
			if err != nil {
				return 0, err
			}

			c.Entries[i] = buf
		}

		return consumed + r.ConsumedBytes(), nil
	}

	// Dictionary mode.
	dictSize, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}

	c.Entries = slices.Grow(c.Entries, int(dictSize))[:dictSize]
	for i := range dictSize {
		ln, err := r.ReadUvarint()
		if err != nil {
			return 0, err
		}

		buf, err := r.ReadBytesView(int(ln))
		if err != nil {
			return 0, err
		}

		c.Entries[i] = buf
	}

	idWidth := 1
	if dictSize > maxDictSize {
		idWidth = 2
	}

	idBuf, err := r.ReadBytesView(rows * idWidth)
	if err != nil {
		return 0, err
	}

	c.IDs = idBuf
	c.IDWidth = idWidth

	return consumed + r.ConsumedBytes(), nil
}

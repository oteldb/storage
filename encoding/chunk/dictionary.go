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
	flagFlat  byte = 0x00 // no dictionary, length-prefixed values stored inline
	flagDict  byte = 0x01 // dictionary mode
	flagFixed byte = 0x02 // no dictionary, one shared width + fixed-width values back-to-back
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

// cellSeq abstracts a byte column's row access for the encode cores, so the [][]byte form and the
// blob+offsets (arrow-style) form share one implementation. The cores are generic over it, so each
// form monomorphizes to direct slice indexing — no per-cell interface call or closure.
type cellSeq interface {
	rows() int
	at(i int) []byte
}

// sliceCells is the [][]byte form of [cellSeq].
type sliceCells [][]byte

func (s sliceCells) rows() int       { return len(s) }
func (s sliceCells) at(i int) []byte { return s[i] }

// blobCells is the blob+offsets form of [cellSeq]: cell i is blob[offsets[i]:offsets[i+1]], with
// len(offsets) == rows+1 and offsets[0] == 0 — the head-buffer byte-column layout, accepted
// directly so a flush encodes straight from the blob without materializing a view per row.
type blobCells struct {
	blob    []byte
	offsets []int32
}

func (b blobCells) rows() int {
	if len(b.offsets) == 0 {
		return 0
	}

	return len(b.offsets) - 1
}

func (b blobCells) at(i int) []byte { return b.blob[b.offsets[i]:b.offsets[i+1]] }

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
	return encodeBytesCells(dst, sliceCells(vals))
}

// EncodeBytesBlob is [EncodeBytes] over the blob+offsets column form (cell i is
// blob[offsets[i]:offsets[i+1]]; len(offsets) == rows+1, offsets[0] == 0). The output is
// byte-identical to EncodeBytes over the equivalent [][]byte, without materializing a view per row.
func EncodeBytesBlob(dst, blob []byte, offsets []int32) []byte {
	return encodeBytesCells(dst, blobCells{blob: blob, offsets: offsets})
}

func encodeBytesCells[C cellSeq](dst []byte, vals C) []byte {
	if vals.rows() == 0 {
		return appendEmpty(dst)
	}

	// Single-pass dictionary build with [pool.ByteIntMap]: xxh3 hash + []byte keys,
	// one probe per value, with row ids and dictionary entry storage reused via a
	// same-lifetime scratch arena.
	m := pool.NewByteIntMap()
	defer m.PutBack()

	scratch := newDictEncodeScratch(vals.rows())
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

// EncodeBytesRaw encodes a byte-string column without a dictionary ([CodecBytesRaw]): when every
// value shares one length it writes a single shared width followed by the values back-to-back
// (the fixed-width form, ideal for id columns); otherwise it falls back to the same length-prefixed
// inline layout as the dictionary codec's flat fallback. The output is a self-describing bytes-column
// stream that [DecodeBytes] and [DecodeBytesDict] read directly.
//
// Layout: [uvarint rows] [1 byte flag] [payload]
//
//	flagFixed payload: [uvarint width] [rows × width bytes]
//	flagFlat  payload: per row [uvarint len][bytes]
func EncodeBytesRaw(dst []byte, vals [][]byte) []byte {
	return encodeBytesRawCells(dst, sliceCells(vals))
}

// EncodeBytesRawBlob is [EncodeBytesRaw] over the blob+offsets column form (see
// [EncodeBytesBlob]); the output is byte-identical to EncodeBytesRaw over the equivalent [][]byte.
func EncodeBytesRawBlob(dst, blob []byte, offsets []int32) []byte {
	return encodeBytesRawCells(dst, blobCells{blob: blob, offsets: offsets})
}

func encodeBytesRawCells[C cellSeq](dst []byte, vals C) []byte {
	n := vals.rows()
	if n == 0 {
		return appendEmpty(dst)
	}

	width, uniform := uniformWidth(vals)

	if uniform {
		dst = slices.Grow(dst, uvarintLen(uint64(n))+1+uvarintLen(uint64(width))+n*width)
		w := bitstream.NewWriter(dst)
		w.WriteUvarint(uint64(n))
		_ = w.WriteByte(flagFixed)
		w.WriteUvarint(uint64(width))

		for i := range n {
			w.WriteBytes(vals.at(i))
		}

		w.PadToByte()

		return w.Bytes()
	}

	return appendFlat(dst, vals)
}

// uniformWidth reports the common length of vals and whether every value (including the first)
// shares it. vals is non-empty.
func uniformWidth[C cellSeq](vals C) (width int, uniform bool) {
	width = len(vals.at(0))
	for i := 1; i < vals.rows(); i++ {
		if len(vals.at(i)) != width {
			return 0, false
		}
	}

	return width, true
}

// appendFlat writes the length-prefixed inline form (flagFlat): per row [uvarint len][bytes].
// Shared by the dictionary codec's >65536-distinct fallback and [EncodeBytesRaw]'s mixed-width path.
func appendFlat[C cellSeq](dst []byte, vals C) []byte {
	n := vals.rows()

	payload := 0
	for i := range n {
		payload += uvarintLen(uint64(len(vals.at(i)))) + len(vals.at(i))
	}

	dst = slices.Grow(dst, uvarintLen(uint64(n))+1+payload)
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(n))
	_ = w.WriteByte(flagFlat)

	for i := range n {
		w.WriteUvarint(uint64(len(vals.at(i))))
		w.WriteBytes(vals.at(i))
	}

	w.PadToByte()

	return w.Bytes()
}

// appendDictEncoded is the shared encode core for [EncodeBytes] and
// [DictEncoder.Encode]. It builds the dictionary into m, reusing the entries and ids
// slices (grown as needed), and appends the encoded column to dst. The grown slices
// are returned so the caller can retain their backing arrays for the next batch.
//
// m is assumed empty (freshly reset); the caller owns its lifecycle.
func appendDictEncoded[C cellSeq](
	dst []byte, vals C, m *pool.ByteIntMap, entries [][]byte, ids []uint16,
) (out []byte, entriesOut [][]byte, idsOut []uint16) {
	n := vals.rows()
	if n == 0 {
		return appendEmpty(dst), entries[:0], ids[:0]
	}

	entries = entries[:0]
	ids = slices.Grow(ids[:0], n)[:n]

	dictEntryBytes := 0
	flat := false

	for i := range n {
		v := vals.at(i)

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
		// Flat fallback (>65536 distinct): no dictionary, store each value inline.
		return appendFlat(dst, vals), entries, ids
	}

	// Dictionary mode. Flag is a full byte so everything after stays byte-aligned.
	rowIDBytes := n
	if size > maxDictSize {
		rowIDBytes *= 2
	}

	dst = slices.Grow(
		dst,
		uvarintLen(uint64(n))+1+uvarintLen(uint64(size))+dictEntryBytes+rowIDBytes,
	)
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(n))
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

	dst, e.entries, e.ids = appendDictEncoded(dst, sliceCells(vals), e.m, e.entries, e.ids)

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

// fixedTotal returns rows*width as an int, rejecting (as a corrupt stream) a product that would
// overflow int — the subsequent [bitstream.Reader.ReadBytesView] bounds-checks the result against
// the actual stream length, so a merely too-large value is caught there.
func fixedTotal(rows int, width uint64) (int, error) {
	const maxInt = int(^uint(0) >> 1)

	if width != 0 && uint64(rows) > uint64(maxInt)/width {
		return 0, errUnexpectedEOF
	}

	return rows * int(width), nil
}

// maxEmptyFixedRows caps the row count of a fixed-width column whose width is zero (an all-empty
// column): it carries no per-row bytes, so without this a corrupt header could request an unbounded
// allocation. Real signal columns are never all-empty under the raw codec, so this defensive bound —
// far above any real column — only ever rejects untrusted/corrupt input, keeping decode panic-safe.
const maxEmptyFixedRows = 1 << 20

// readView reads an ln-byte view, rejecting (rather than panicking on) a length from an untrusted
// uvarint that would overflow int when narrowed. An ln that merely exceeds the remaining stream is
// caught by [bitstream.Reader.ReadBytesView] itself.
func readView(r *bitstream.Reader, ln uint64) ([]byte, error) {
	const maxInt = uint64(^uint(0) >> 1)

	if ln > maxInt {
		return nil, errUnexpectedEOF
	}

	return r.ReadBytesView(int(ln))
}

// ensureRows grows dst to exactly rows elements, reusing capacity.
func ensureRows(dst [][]byte, rows int) [][]byte {
	if cap(dst) < rows {
		dst = resize(dst, rows)
	}

	return dst[:rows]
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

	if rows < 0 { // a row count above maxInt wraps negative via int(uvarint): corrupt
		return dst, 0, errUnexpectedEOF
	}

	flag, err := r.ReadByte()
	if err != nil {
		return dst, 0, err
	}

	// avail bounds untrusted counts: every flat/dict row and every dict entry consumes at least one
	// downstream byte, so a count exceeding the bytes left after the flag is a corrupt header — reject
	// it before allocating (the dst/scratch sizing below trusts the validated count).
	avail := len(src) - consumed - 1

	switch flag {
	case flagFlat:
		dst, err = decodeFlatGather(&r, dst, rows, avail)
	case flagFixed:
		dst, err = decodeFixedGather(&r, dst, rows)
	default:
		dst, err = decodeDictGather(&r, dst, rows, avail)
	}

	if err != nil {
		return dst, 0, err
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

// decodeFlatGather fills dst from a flagFlat payload: per row [uvarint len][bytes].
func decodeFlatGather(r *bitstream.Reader, dst [][]byte, rows, avail int) ([][]byte, error) {
	if rows > avail { // each row consumes at least one downstream byte
		return dst, errUnexpectedEOF
	}

	dst = ensureRows(dst, rows)
	for i := range rows {
		ln, err := r.ReadUvarint()
		if err != nil {
			return dst, err
		}

		buf, err := readView(r, ln)
		if err != nil {
			return dst, err
		}

		dst[i] = buf
	}

	return dst, nil
}

// decodeFixedGather fills dst from a flagFixed payload: [uvarint width][rows×width bytes].
func decodeFixedGather(r *bitstream.Reader, dst [][]byte, rows int) ([][]byte, error) {
	width, err := r.ReadUvarint()
	if err != nil {
		return dst, err
	}

	if width == 0 && rows > maxEmptyFixedRows {
		return dst, errUnexpectedEOF
	}

	total, err := fixedTotal(rows, width)
	if err != nil {
		return dst, err
	}

	block, err := r.ReadBytesView(total)
	if err != nil {
		return dst, err
	}

	dst = ensureRows(dst, rows)
	w := int(width)
	for i := range rows {
		dst[i] = block[i*w : (i+1)*w]
	}

	return dst, nil
}

// decodeDictGather fills dst from a flagDict payload, gathering per-row values from the dictionary.
func decodeDictGather(r *bitstream.Reader, dst [][]byte, rows, avail int) ([][]byte, error) {
	if rows > avail {
		return dst, errUnexpectedEOF
	}

	dst = ensureRows(dst, rows)

	dictSize, err := r.ReadUvarint()
	if err != nil {
		return dst, err
	}

	if dictSize > uint64(avail) {
		return dst, errUnexpectedEOF
	}

	scratch := newDictDecodeScratch(int(dictSize))
	defer scratch.putBack()

	entries := scratch.entries
	for i := range dictSize {
		ln, err := r.ReadUvarint()
		if err != nil {
			return dst, err
		}

		buf, err := readView(r, ln)
		if err != nil {
			return dst, err
		}

		entries[i] = buf
	}

	idWidth := 1
	if dictSize > maxDictSize {
		idWidth = 2
	}

	idBuf, err := r.ReadBytesView(rows * idWidth)
	if err != nil {
		return dst, err
	}

	for i := range rows {
		id := idAt(idBuf, i, idWidth)
		if id >= len(entries) { // a row id past the dictionary is corrupt
			return dst, errUnexpectedEOF
		}

		dst[i] = entries[id]
	}

	return dst, nil
}

// idAt returns the idWidth-byte big-endian row id at index i in a packed id array.
func idAt(idBuf []byte, i, idWidth int) int {
	if idWidth == 1 {
		return int(idBuf[i])
	}

	return int(uint16(idBuf[i*2])<<8 | uint16(idBuf[i*2+1]))
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

	if rows < 0 { // a row count above maxInt wraps negative via int(uvarint): corrupt
		return 0, errUnexpectedEOF
	}

	flag, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	// See [DecodeBytes]: reject untrusted counts that exceed the downstream bytes before allocating.
	avail := len(src) - consumed - 1

	switch flag {
	case flagFlat:
		err = c.decodeFlat(&r, rows, avail)
	case flagFixed:
		err = c.decodeFixed(&r, rows)
	default:
		err = c.decodeDict(&r, rows, avail)
	}

	if err != nil {
		return 0, err
	}

	return consumed + r.ConsumedBytes(), nil
}

// decodeFlat fills c from a flagFlat payload: one entry per row, indexed directly (IDWidth 0).
func (c *DictColumn) decodeFlat(r *bitstream.Reader, rows, avail int) error {
	if rows > avail {
		return errUnexpectedEOF
	}

	c.Entries = slices.Grow(c.Entries, rows)[:rows]
	for i := range rows {
		ln, err := r.ReadUvarint()
		if err != nil {
			return err
		}

		buf, err := readView(r, ln)
		if err != nil {
			return err
		}

		c.Entries[i] = buf
	}

	return nil
}

// decodeFixed fills c from a flagFixed payload: one shared width, values sliced into per-row views.
func (c *DictColumn) decodeFixed(r *bitstream.Reader, rows int) error {
	width, err := r.ReadUvarint()
	if err != nil {
		return err
	}

	if width == 0 && rows > maxEmptyFixedRows {
		return errUnexpectedEOF
	}

	total, err := fixedTotal(rows, width)
	if err != nil {
		return err
	}

	block, err := r.ReadBytesView(total)
	if err != nil {
		return err
	}

	c.Entries = slices.Grow(c.Entries, rows)[:rows]
	w := int(width)
	for i := range rows {
		c.Entries[i] = block[i*w : (i+1)*w]
	}

	return nil
}

// decodeDict fills c's dictionary and validated id array from a flagDict payload, deferring the
// per-row gather to [DictColumn.At].
func (c *DictColumn) decodeDict(r *bitstream.Reader, rows, avail int) error {
	if rows > avail {
		return errUnexpectedEOF
	}

	dictSize, err := r.ReadUvarint()
	if err != nil {
		return err
	}

	if dictSize > uint64(avail) {
		return errUnexpectedEOF
	}

	c.Entries = slices.Grow(c.Entries, int(dictSize))[:dictSize]
	for i := range dictSize {
		ln, err := r.ReadUvarint()
		if err != nil {
			return err
		}

		buf, err := readView(r, ln)
		if err != nil {
			return err
		}

		c.Entries[i] = buf
	}

	idWidth := 1
	if dictSize > maxDictSize {
		idWidth = 2
	}

	idBuf, err := r.ReadBytesView(rows * idWidth)
	if err != nil {
		return err
	}

	// Validate every row id against the dictionary now so the deferred [DictColumn.At] gather is
	// panic-safe on corrupt input (one cheap integer scan, no per-row []byte materialization).
	if err := validateIDs(idBuf, idWidth, len(c.Entries)); err != nil {
		return err
	}

	c.IDs = idBuf
	c.IDWidth = idWidth

	return nil
}

// validateIDs reports a corrupt stream if any idWidth-byte big-endian id in idBuf references an entry
// at or beyond n (the dictionary size).
func validateIDs(idBuf []byte, idWidth, n int) error {
	if idWidth == 1 {
		for _, id := range idBuf {
			if int(id) >= n {
				return errUnexpectedEOF
			}
		}

		return nil
	}

	for i := 0; i+1 < len(idBuf); i += 2 {
		if int(uint16(idBuf[i])<<8|uint16(idBuf[i+1])) >= n {
			return errUnexpectedEOF
		}
	}

	return nil
}

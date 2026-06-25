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
func EncodeBytes(dst []byte, vals [][]byte) []byte {
	if len(vals) == 0 {
		w := bitstream.NewWriter(dst)
		w.WriteUvarint(0)
		w.PadToByte()

		return w.Bytes()
	}

	// Single-pass dictionary build with [pool.ByteIntMap]: xxh3 hash + []byte keys,
	// one probe per value, with row ids and dictionary entry storage reused via a
	// same-lifetime scratch arena.
	m := pool.NewByteIntMap()
	defer m.PutBack()

	scratch := newDictEncodeScratch(len(vals))
	defer scratch.putBack()

	dictEntryBytes := 0
	flat := false

	for i, v := range vals {
		id, existed := m.PutOrGet(v, len(scratch.entries))
		if !existed {
			if len(scratch.entries) == 65536 {
				flat = true

				break
			}

			scratch.entries = append(scratch.entries, v)
			dictEntryBytes += uvarintLen(uint64(len(v))) + len(v)
		}

		scratch.ids[i] = uint16(id)
	}

	size := len(scratch.entries)

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

		return w.Bytes()
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
	for _, e := range scratch.entries {
		w.WriteUvarint(uint64(len(e)))
		w.WriteBytes(e)
	}
	// Write row ids directly into the writer buffer (byte-aligned here).
	if size <= maxDictSize {
		idBytes := w.AppendBytes(len(scratch.ids))
		for i, id := range scratch.ids {
			idBytes[i] = byte(id)
		}
	} else {
		idBytes := w.AppendBytes(len(scratch.ids) * 2)
		for i, id := range scratch.ids {
			idBytes[i*2] = byte(id >> 8)
			idBytes[i*2+1] = byte(id)
		}
	}

	w.PadToByte()

	return w.Bytes()
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

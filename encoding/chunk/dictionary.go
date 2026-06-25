package chunk

import "github.com/oteldb/storage/encoding/bitstream"

// maxDictSize is the maximum dictionary cardinality for the 1-byte-id fast path.
// Values above this fall back to a 2-byte id. Matches VictoriaLogs/Parquet dict
// thresholds (DESIGN.md §6: "≤256 distinct → 1 byte/row").
const maxDictSize = 256

// Dictionary format flag bytes (full byte, not a single bit, so all subsequent bulk
// writes stay byte-aligned for the fast [bitstream.Writer.AppendString]/
// [bitstream.Writer.WriteBytes] paths).
const (
	flagFlat byte = 0x00 // no dictionary, strings stored inline
	flagDict byte = 0x01 // dictionary mode
)

// EncodeStrings appends a dictionary-encoded string column to dst and returns the
// extended slice (DESIGN.md §6, §14 M0).
//
// Layout: [uvarint rows] [1 byte flag] [payload]
//
// flagDict payload: [uvarint dictSize] [dict entries: per entry uvarint len + bytes]
// [row ids: 1 byte each if dictSize ≤ 256, else 2 bytes big-endian each]
//
// flagFlat payload: per row [uvarint len][bytes] (the flat fallback for >65536 distinct).
//
// Dictionary entries are deduplicated in insertion order. The full-byte flag (not a
// single bit) keeps the writer byte-aligned so dictionary-entry string data and row-id
// arrays are written as bulk appends (zero-copy via append([]byte, string...)).
func EncodeStrings(dst []byte, vals []string) []byte {
	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(len(vals)))

	if len(vals) == 0 {
		w.PadToByte()
		return w.Bytes()
	}

	// Single-pass dictionary build: one map lookup per string, collect entries in
	// insertion order to avoid a reverse map iteration.
	dict := make(map[string]int, min(len(vals), 256))
	entries := make([]string, 0, min(len(vals), 256))
	ids := make([]int, len(vals))
	for i, s := range vals {
		id, ok := dict[s]
		if !ok {
			id = len(entries)
			dict[s] = id
			entries = append(entries, s)
		}
		ids[i] = id
	}
	size := len(entries)

	if size > 65536 {
		// Flat fallback: no dictionary, store each string inline.
		_ = w.WriteByte(flagFlat)
		for _, s := range vals {
			w.WriteUvarint(uint64(len(s)))
			w.AppendString(s)
		}
		w.PadToByte()
		return w.Bytes()
	}

	// Dictionary mode. Flag is a full byte so everything after stays byte-aligned.
	_ = w.WriteByte(flagDict)
	w.WriteUvarint(uint64(size))
	// Write dictionary entries: length-prefixed (uvarint) + string bytes (bulk append,
	// zero-copy via append([]byte, string...)).
	for _, s := range entries {
		w.WriteUvarint(uint64(len(s)))
		w.AppendString(s)
	}
	// Write row ids as a bulk byte array (byte-aligned here).
	if size <= maxDictSize {
		idBytes := make([]byte, len(ids))
		for i, id := range ids {
			idBytes[i] = byte(id)
		}
		w.WriteBytes(idBytes)
	} else {
		idBytes := make([]byte, len(ids)*2)
		for i, id := range ids {
			idBytes[i*2] = byte(id >> 8)
			idBytes[i*2+1] = byte(id)
		}
		w.WriteBytes(idBytes)
	}
	w.PadToByte()
	return w.Bytes()
}

// DecodeStrings decodes a dictionary-encoded string column from src into dst.
func DecodeStrings(dst []string, src []byte) ([]string, int, error) {
	r, rows, consumed, err := readHeader(src)
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
			buf := make([]byte, ln)
			if err := r.ReadBytes(buf); err != nil {
				return dst, 0, err
			}
			dst[i] = string(buf)
		}
		return dst, consumed + r.ConsumedBytes(), nil
	}

	// Dictionary mode.
	dictSize, err := r.ReadUvarint()
	if err != nil {
		return dst, 0, err
	}
	entries := make([]string, dictSize)
	for i := range dictSize {
		ln, err := r.ReadUvarint()
		if err != nil {
			return dst, 0, err
		}
		buf := make([]byte, ln)
		if err := r.ReadBytes(buf); err != nil {
			return dst, 0, err
		}
		entries[i] = string(buf)
	}

	if dictSize <= maxDictSize {
		idBuf := make([]byte, rows)
		if err := r.ReadBytes(idBuf); err != nil {
			return dst, 0, err
		}
		for i := range rows {
			dst[i] = entries[idBuf[i]]
		}
	} else {
		idBuf := make([]byte, rows*2)
		if err := r.ReadBytes(idBuf); err != nil {
			return dst, 0, err
		}
		for i := range rows {
			dst[i] = entries[(uint16(idBuf[i*2])<<8)|uint16(idBuf[i*2+1])]
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

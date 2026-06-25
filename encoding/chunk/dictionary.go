package chunk

import "github.com/oteldb/storage/encoding/bitstream"

// maxDictSize is the maximum dictionary cardinality for the 1-byte-id fast path.
// Values above this fall back to a 2-byte id. Matches VictoriaLogs/Parquet dict
// thresholds (DESIGN.md §6: "≤256 distinct → 1 byte/row").
const maxDictSize = 256

// EncodeStrings appends a dictionary-encoded string column to dst and returns the
// extended slice (DESIGN.md §6, §14 M0).
//
// Layout: [uvarint rows] [uvarint dictSize] [dict entries] [row ids]
//
// For dictSize ≤ 256, each row id is 1 byte. For dictSize ≤ 65536, each row id is
// 2 bytes (big-endian). Above that, the dictionary is abandoned and values are stored
// as length-prefixed (uvarint) strings — the "flat" fallback.
//
// Dictionary entries are length-prefixed (uvarint length + bytes), concatenated.
// The flat fallback is: [uvarint rows] then per row [uvarint len][bytes].
func EncodeStrings(dst []byte, vals []string) []byte {
	// Build the dictionary.
	dict := make(map[string]int, len(vals))
	ids := make([]int, len(vals))
	size := 0
	for i, s := range vals {
		if _, ok := dict[s]; !ok {
			dict[s] = size
			size++
		}
		ids[i] = dict[s]
	}

	w := bitstream.NewWriter(dst)
	w.WriteUvarint(uint64(len(vals)))

	if size == 0 {
		w.PadToByte()
		return w.Bytes()
	}

	if size > 65536 {
		// Flat fallback: no dictionary, store each string inline.
		w.WriteBit(false) // flag: no dictionary
		for _, s := range vals {
			w.WriteUvarint(uint64(len(s)))
			for _, b := range []byte(s) {
				_ = w.WriteByte(b)
			}
		}
		w.PadToByte()
		return w.Bytes()
	}

	// Dictionary mode.
	w.WriteBit(true) // flag: dictionary present
	w.WriteUvarint(uint64(size))
	// Write dictionary entries (deduplicated, in insertion order).
	entries := make([]string, size)
	for s, id := range dict {
		entries[id] = s
	}
	for _, s := range entries {
		w.WriteUvarint(uint64(len(s)))
		for _, b := range []byte(s) {
			_ = w.WriteByte(b)
		}
	}
	// Write row ids.
	if size <= maxDictSize {
		for _, id := range ids {
			_ = w.WriteByte(byte(id))
		}
	} else {
		for _, id := range ids {
			_ = w.WriteByte(byte(id >> 8))
			_ = w.WriteByte(byte(id))
		}
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

	hasDict, err := r.ReadBit()
	if err != nil {
		return dst, 0, err
	}

	if !hasDict {
		// Flat fallback.
		for i := range rows {
			ln, err := r.ReadUvarint()
			if err != nil {
				return dst, 0, err
			}
			buf := make([]byte, ln)
			for j := range buf {
				b, err := r.ReadBits(8)
				if err != nil {
					return dst, 0, err
				}
				buf[j] = byte(b)
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
		for j := range buf {
			b, err := r.ReadBits(8)
			if err != nil {
				return dst, 0, err
			}
			buf[j] = byte(b)
		}
		entries[i] = string(buf)
	}

	if dictSize <= maxDictSize {
		for i := range rows {
			id, err := r.ReadBits(8)
			if err != nil {
				return dst, 0, err
			}
			dst[i] = entries[id]
		}
	} else {
		for i := range rows {
			hi, err := r.ReadBits(8)
			if err != nil {
				return dst, 0, err
			}
			lo, err := r.ReadBits(8)
			if err != nil {
				return dst, 0, err
			}
			dst[i] = entries[(hi<<8)|lo]
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

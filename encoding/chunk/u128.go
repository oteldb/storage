package chunk

import (
	"encoding/binary"
	"slices"
)

// U128 is an unsigned 128-bit value (a SeriesID at the storage boundary). Comparable, so
// runs of equal ids are detected directly.
type U128 struct {
	Hi, Lo uint64
}

// EncodeU128 appends a run-length-encoded [U128] column to dst (CodecID128). The id sort
// key of a metric part is long runs of one id (all of a series' samples are contiguous),
// so RLE stores a distinct id + run length per run — far smaller than storing every row.
//
// Layout: [uvarint rows] then per run [u128 id big-endian][uvarint runLength].
func EncodeU128(dst []byte, vals []U128) []byte {
	w, _ := writeHeader(dst, len(vals))

	for i := 0; i < len(vals); {
		j := i + 1
		for j < len(vals) && vals[j] == vals[i] {
			j++
		}

		b := w.AppendBytes(16)
		binary.BigEndian.PutUint64(b[:8], vals[i].Hi)
		binary.BigEndian.PutUint64(b[8:], vals[i].Lo)
		w.WriteUvarint(uint64(j - i))

		i = j
	}

	w.PadToByte()

	return w.Bytes()
}

// U128Run is one run of a run-length-encoded [U128] column: a value repeated Count times.
type U128Run struct {
	Value U128
	Count int
}

// EncodeU128Runs is [EncodeU128] over an already run-length-encoded column, so a writer producing
// its rows a run at a time never materializes the expanded []U128 just to have the encoder collapse
// it again. Zero-count runs are skipped and adjacent equal runs coalesced, so the output is
// byte-identical to EncodeU128 over the expanded column.
func EncodeU128Runs(dst []byte, runs []U128Run) []byte {
	rows, size, groups, first := u128RunsSize(runs, len(dst))
	dst = slices.Grow(dst, size)
	dst = binary.AppendUvarint(dst, uint64(rows))
	if groups == 0 {
		return dst
	}
	if groups == 1 {
		dst = binary.BigEndian.AppendUint64(dst, first.Hi)
		dst = binary.BigEndian.AppendUint64(dst, first.Lo)
		return binary.AppendUvarint(dst, uint64(rows))
	}

	for i := 0; i < len(runs); {
		if runs[i].Count <= 0 {
			i++

			continue
		}

		n := runs[i].Count

		j := i + 1
		for j < len(runs) && (runs[j].Count <= 0 || runs[j].Value == runs[i].Value) {
			n += max(runs[j].Count, 0)
			j++
		}

		dst = binary.BigEndian.AppendUint64(dst, runs[i].Value.Hi)
		dst = binary.BigEndian.AppendUint64(dst, runs[i].Value.Lo)
		dst = binary.AppendUvarint(dst, uint64(n))

		i = j
	}

	return dst
}

func u128RunsSize(runs []U128Run, prefix int) (rows, size, groups int, first U128) {
	const maxInt = int(^uint(0) >> 1)
	available := maxInt - prefix
	for i := 0; i < len(runs); {
		if runs[i].Count <= 0 {
			i++
			continue
		}
		if groups == 0 {
			first = runs[i].Value
		}
		groups++
		n := runs[i].Count
		j := i + 1
		for j < len(runs) && (runs[j].Count <= 0 || runs[j].Value == runs[i].Value) {
			n += max(runs[j].Count, 0)
			j++
		}
		bytes := 16 + uvarintLen(uint64(n))
		if bytes > available-size {
			panic("chunk: U128 run encoding exceeds addressable size")
		}
		size += bytes
		rows += n
		i = j
	}
	header := uvarintLen(uint64(rows))
	if header > available-size {
		panic("chunk: U128 run encoding exceeds addressable size")
	}
	return rows, size + header, groups, first
}

// DecodeU128 decodes a [U128] column into dst (reusing its capacity), returning the result and bytes
// consumed. It must hold exactly rows values: a stream whose header states another count is corrupt.
//
// rows comes from the caller because the stream cannot bound it: one 17-byte run encodes any row
// count, so trusting the header would let a few corrupt bytes size the output.
func DecodeU128(dst []U128, src []byte, rows int) ([]U128, int, error) {
	r, rows, consumed, err := readHeaderRows(src, rows)
	if err != nil {
		return dst, 0, err
	}

	dst = dst[:0]
	if cap(dst) < rows {
		dst = make([]U128, 0, rows)
	}

	for len(dst) < rows {
		raw, err := r.ReadBytesView(16)
		if err != nil {
			return dst, 0, err
		}

		id := U128{Hi: binary.BigEndian.Uint64(raw[:8]), Lo: binary.BigEndian.Uint64(raw[8:])}

		runLen, err := r.ReadUvarint()
		if err != nil {
			return dst, 0, err
		}

		if runLen == 0 || runLen > uint64(rows-len(dst)) {
			return dst, 0, errUnexpectedEOF
		}

		for range runLen {
			dst = append(dst, id)
		}
	}

	return dst, consumed + r.ConsumedBytes(), nil
}

package recordengine

import (
	"context"

	"github.com/oteldb/storage/encoding/chunk"
)

// decodedPart is one source part decoded for a merge: the fixed-width columns as plain slices, and each
// byte column kept dict-compressed where possible ([mergeByteCol]). A merge interleaves streams across
// all selected parts, so every selected part stays resident during the stream sweep; holding the byte
// columns dict-compressed (rather than expanding each to a full uncompressed blob, as the fetch-path
// readCols does) keeps that resident set small when values repeat — the common log case (templated
// bodies, low-cardinality attributes). Selection already bounds how many parts this covers; this bounds
// the per-part constant.
type decodedPart struct {
	ts    []int64
	ints  [][]int64
	bytes []mergeByteCol
}

// mergeByteCol holds one source byte column of a merge, either dict-compressed or, for the dictionary's
// flat fallback, as a packed [byteCol].
type mergeByteCol struct {
	dict *chunk.DictColumn // non-nil ⇒ dict-compressed: Σ(unique entries) + packed ids
	flat byteCol           // used when dict == nil (the flat fallback)
}

// newMergeByteCol keeps a real dictionary (IDWidth > 0) compressed — repeated cells dedup to a small
// entry set, so a part holds far less than its expanded blob. The flat fallback (IDWidth 0: a part with
// > 65536 distinct values, where the writer found no dedup) is materialized into a packed byteCol
// instead, because its dict form carries one []byte header per row — larger than offsets+blob. So the
// merge is never worse than the old expand-everything path and much smaller when values repeat.
func newMergeByteCol(dc *chunk.DictColumn) mergeByteCol {
	if dc.IDWidth != 0 {
		return mergeByteCol{dict: dc}
	}

	n := dc.Len()

	// The flat form holds one entry per row, so the expanded blob's exact size is a walk over the
	// entry headers — no data touched. Reserving it keeps the copy below from re-growing (and
	// re-copying) the blob as it fills.
	blob := 0
	for _, e := range dc.Entries {
		blob += len(e)
	}

	bc := byteCol{}
	bc.ensureBytes(n, blob)

	for i := range n {
		bc.appendCell(dc.At(i))
	}

	return mergeByteCol{flat: bc}
}

// expandedBytes is the blob the column's cells occupy once materialized: what it already holds in
// the flat form, and the sum of its rows' entry lengths for a dictionary. It sizes the merge output
// buffer, which would otherwise grow each byte column out of nothing. Walking a dictionary costs one
// table lookup per row over the packed ids — no cell is touched — against the ~log₂(size) full blob
// re-copies the sizing avoids.
func (m *mergeByteCol) expandedBytes() int64 {
	if m.dict == nil {
		return m.flat.byteSize()
	}

	dc := m.dict

	lens := make([]int32, len(dc.Entries))
	for i, e := range dc.Entries {
		lens[i] = int32(len(e))
	}

	var total int64

	if dc.IDWidth == 1 {
		for _, id := range dc.IDs {
			total += int64(lens[id])
		}

		return total
	}

	for i := 0; i+1 < len(dc.IDs); i += 2 {
		total += int64(lens[uint16(dc.IDs[i])<<8|uint16(dc.IDs[i+1])])
	}

	return total
}

// at returns a view of cell i (aliasing the dictionary entry or the flat blob; valid until the flat
// blob's next append, which the merge never does after decode).
func (m *mergeByteCol) at(i int) []byte {
	if m.dict != nil {
		return m.dict.At(i)
	}

	return m.flat.at(i)
}

// readForMerge decodes the whole part for a merge: the timestamp and int columns as int64 slices and
// each byte column via [newMergeByteCol]. It reads off the engine lock (the part is ref-held live by
// the merge until publish), so a fetch and this decode never race a delete.
func (p *part) readForMerge(ctx context.Context) (*decodedPart, error) {
	d := &decodedPart{
		ints:  make([][]int64, p.schema.numInts()),
		bytes: make([]mergeByteCol, p.schema.numBytes()),
	}

	var err error
	if d.ts, err = p.readInt64(ctx, colTs, nil, nil); err != nil {
		return nil, err
	}

	for k := range d.ints {
		if d.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, nil, nil); err != nil {
			return nil, err
		}
	}

	for k := range d.bytes {
		col, err := p.reader.Column(ctx, p.schema.byteColumn(k).Name)
		if err != nil {
			return nil, err
		}

		dc, err := col.Bytes()
		if err != nil {
			return nil, err
		}

		d.bytes[k] = newMergeByteCol(dc)
	}

	return d, nil
}

// decodedShape sizes a merge's output buffer from its decoded sources: their total row count and,
// per byte column, the blob their cells expand to. capBytes (0 ⇒ no seal) scales it down to what one
// output part holds, so a merge emitting many parts does not size its buffer for all of them.
//
// The estimate is an upper bound — retention drops rows the sources still carry — and the buffer
// grows past it as normal if it is short.
func decodedShape(decoded []*decodedPart, capBytes int64) (rows int, blob []int) {
	if len(decoded) == 0 {
		return 0, nil
	}

	blob = make([]int, len(decoded[0].bytes))

	var total int64

	for _, d := range decoded {
		rows += len(d.ts)
		total += int64(len(d.ts)) * int64(8+8*len(d.ints)+streamIDBytes)

		for k := range d.bytes {
			n := d.bytes[k].expandedBytes()
			blob[k] += int(n)
			total += n
		}
	}

	if capBytes <= 0 || total <= capBytes {
		return rows, blob
	}

	scale := float64(capBytes) / float64(total)
	rows = int(float64(rows) * scale)

	for k := range blob {
		blob[k] = int(float64(blob[k]) * scale)
	}

	return rows, blob
}

// appendMergeRow appends row i of a decoded source part to c (every schema column; the merge rewrites
// them all). Byte cells are copied into c's blob, so they no longer alias the source part.
func (c *recordCols) appendMergeRow(d *decodedPart, i int) {
	c.ts = append(c.ts, d.ts[i])
	c.noteTS(d.ts[i])

	for k := range c.ints {
		c.ints[k] = append(c.ints[k], d.ints[k][i])
	}

	for k := range c.bytes {
		c.bytes[k].appendCell(d.bytes[k].at(i))
	}
}

// appendMergeWindow appends rows [rng.start, rng.end) of d whose timestamp is in [start, end] to acc.
func appendMergeWindow(acc *recordCols, d *decodedPart, rng rowRange, start, end int64) {
	for i := rng.start; i < rng.end; i++ {
		if d.ts[i] >= start && d.ts[i] <= end {
			acc.appendMergeRow(d, i)
		}
	}
}

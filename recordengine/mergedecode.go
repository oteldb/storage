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
	bc := byteCol{}
	bc.ensure(n)

	for i := range n {
		bc.appendCell(dc.At(i))
	}

	return mergeByteCol{flat: bc}
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
	if d.ts, err = p.readInt64(ctx, colTs, nil); err != nil {
		return nil, err
	}

	for k := range d.ints {
		if d.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, nil); err != nil {
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

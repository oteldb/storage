package recordengine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/encoding/chunk"
)

// decodedPart is one source part decoded whole for a merge, the fallback for a part a forward cursor
// cannot read ([forwardReadable]): the fixed-width columns as plain slices, and each byte column kept
// dict-compressed where possible ([mergeByteCol]). It stays resident for the whole stream sweep, so
// holding the byte columns dict-compressed rather than expanded keeps it small when values repeat.
type decodedPart struct {
	ts    []int64
	ints  [][]int64
	bytes []mergeByteCol
	// remap[k] maps byte column k's dictionary ids to the merge's union ids, for a column on the
	// split carry.
	remap [][]int32
	// tsSorted is whether every stream's rows are ts-ascending, as both part writers leave them. Only then
	// may a merge binary-search a stream's window: on a part that breaks the order, a search would skip
	// in-window rows, and the merge would retire the part that held them.
	tsSorted bool
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

// entryID returns row i's index into the column's dictionary entry table. Only defined for the dict
// form.
func (m *mergeByteCol) entryID(i int) int32 { return dictID(m.dict, i) }

// dictID returns row i's entry id in a dictionary-encoded column (IDWidth 1 or 2).
func dictID(dc *chunk.DictColumn, i int) int32 {
	if dc.IDWidth == 1 {
		return int32(dc.IDs[i])
	}

	return int32(uint16(dc.IDs[i*2])<<8 | uint16(dc.IDs[i*2+1]))
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

	d.tsSorted = streamsTSSorted(d.ts, p.ranges)

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

// appendMergeRow appends row i of decoded source part d to c (every schema column; the merge rewrites
// them all). A split byte column takes the row's dictionary id through the union remap — no cell is
// read or copied; a flat one copies the cell into c's blob, so it no longer aliases the source part.
func (c *recordCols) appendMergeRow(d *decodedPart, i int) {
	c.ts = append(c.ts, d.ts[i])
	c.noteTS(d.ts[i])

	for k := range c.ints {
		c.ints[k] = append(c.ints[k], d.ints[k][i])
	}

	for k := range c.bytes {
		if sc := c.splitAt(k); sc != nil {
			sc.append(d.remap[k][d.bytes[k].entryID(i)])

			continue
		}

		c.bytes[k].appendCell(d.bytes[k].at(i))
	}
}

// appendMergeWindow appends rows [rng.start, rng.end) of d whose timestamp is in [start, end] to acc.
func appendMergeWindow(acc *recordCols, d *decodedPart, rng rowRange, start, end int64) {
	if d.tsSorted {
		w := tsWindow(d.ts, rng, start, end)
		for i := w.start; i < w.end; i++ {
			acc.appendMergeRow(d, i)
		}

		return
	}

	for i := rng.start; i < rng.end; i++ {
		if d.ts[i] >= start && d.ts[i] <= end {
			acc.appendMergeRow(d, i)
		}
	}
}

func streamsTSSorted(ts []int64, ranges []streamRange) bool {
	for _, r := range ranges {
		if r.end > len(ts) || !slices.IsSorted(ts[r.start:r.end]) {
			return false
		}
	}

	return true
}

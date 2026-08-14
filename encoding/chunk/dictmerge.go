package chunk

import "github.com/oteldb/storage/pool"

// DictMerger accumulates the decoded granules of a block-framed bytes column into one [DictColumn]
// spanning their rows.
//
// A block-framed bytes column encodes each granule independently, so each carries its own
// dictionary and its ids mean nothing outside it. A reader that decodes several granules must
// therefore remap them into a shared dictionary — a column of concatenated per-granule dictionaries
// is not a [DictColumn], and handing the query engine one column per granule would push the
// per-granule structure into every caller.
//
// Merging rather than flattening is what preserves the per-distinct-entry predicate memo: with a
// dictionary, a regex or attribute lookup is evaluated once per distinct value; without one it runs
// per row. That memo is the reason a low-cardinality column is worth dictionary-encoding at all, so
// a merge that flattened would give back most of what the encoding bought.
//
// The merged dictionary degrades to the flat form (one entry per row, IDWidth 0) as soon as it
// cannot pay: a source granule that is itself flat, or a merged dictionary past the 2-byte id
// width. That mirrors what the encoder does within one granule, so the merged column is always a
// shape [DictColumn.At] already handles.
type DictMerger struct {
	m       *pool.ByteIntMap
	entries [][]byte
	ids     []int32 // global entry id per row; unused once flat
	flat    [][]byte
	isFlat  bool

	// scatter mode: the result spans the whole column and each granule lands at its own offset. See
	// [DictMerger.Scatter].
	scatter bool
	rows    int
	at      int // row offset of the next Append, in scatter mode
}

// maxDictEntries is the largest dictionary a 2-byte row id can address; past it the merged column
// degrades to the flat form, as the single-granule encoder does.
const maxDictEntries = 1 << 16

// Scatter puts the merge in whole-column mode: the result spans the column, and each appended
// granule lands at its own offset rather than being packed against its predecessor. Rows no
// granule covers decode to an empty value.
//
// This is what lets a pruned read keep working in *part row indices*. A packed result would
// renumber every row a query already located through the part's row-range index and its marks, so
// every caller would have to translate; the int64 path avoids that by decoding into the destination
// at absolute offsets, and this gives bytes columns the same property. The cost is an id array
// sized to the column rather than to the selection — one or two bytes per skipped row, against the
// values those rows would otherwise have decoded.
func (d *DictMerger) Scatter(rows int) {
	d.scatter, d.rows = true, rows
}

// Reset returns m to its empty state, keeping its buffers for reuse.
func (d *DictMerger) Reset() {
	if d.m != nil {
		d.m.PutBack()
		d.m = nil
	}

	d.entries, d.ids, d.flat, d.isFlat = d.entries[:0], d.ids[:0], d.flat[:0], false
	d.scatter, d.rows, d.at = false, 0, 0
}

// AppendAt adds every row of c to the merge, landing at row offset off. Scatter mode only.
func (d *DictMerger) AppendAt(c *DictColumn, off int) {
	d.at = off
	d.Append(c)
}

// Append adds every row of c to the merge. The byte slices of c are retained, not copied, so the
// caller must keep c's backing buffer alive until [DictMerger.Build] — for a block-framed column
// that means the decompressed frames must not be recycled underneath the walk.
func (d *DictMerger) Append(c *DictColumn) {
	rows := c.Len()
	if rows == 0 {
		return
	}

	// A flat source has no dictionary to merge: its values are already per-row, and probing them
	// into a shared dictionary would build one the size of the column to no purpose. This is the
	// common case for a high-cardinality column (a log body), where every granule encodes flat.
	if c.IDWidth == 0 {
		d.degrade()
	}

	if d.isFlat {
		d.putFlat(c, rows)

		return
	}

	if d.m == nil {
		d.m = pool.NewByteIntMap()

		// Scatter mode reserves id 0 for the rows no granule covers, so a skipped row decodes to an
		// empty value rather than indexing past the dictionary.
		if d.scatter && len(d.entries) == 0 {
			d.entries = append(d.entries, nil)
			d.m.Put(nil, 0)
		}
	}

	// Remap the granule's local ids to global ones once per *entry*, not per row: a granule's rows
	// index a dictionary of at most 65536 entries, so the per-row loop below is an array lookup.
	local := make([]int32, len(c.Entries))

	for i, e := range c.Entries {
		id, ok := d.m.Get(e)
		if !ok {
			if len(d.entries) >= maxDictEntries {
				d.degrade()
				d.putFlat(c, rows)

				return
			}

			id = len(d.entries)
			d.m.Put(e, id)
			d.entries = append(d.entries, e)
		}

		local[i] = int32(id)
	}

	if d.scatter {
		d.grow(d.at, rows)

		for r := range rows {
			d.ids[d.at+r] = local[c.localID(r)]
		}

		return
	}

	for r := range rows {
		d.ids = append(d.ids, local[c.localID(r)])
	}
}

// Build returns the merged column. Its byte slices alias those of the appended granules.
func (d *DictMerger) Build() *DictColumn {
	if d.m != nil {
		d.m.PutBack()
		d.m = nil
	}

	if d.scatter {
		d.grow(d.rows, 0)
	}

	if d.isFlat {
		return &DictColumn{Entries: d.flat}
	}

	if len(d.ids) == 0 {
		return &DictColumn{}
	}

	// One byte per row while the dictionary fits, matching what the single-granule encoder picks.
	if len(d.entries) <= 256 {
		ids := make([]byte, len(d.ids))
		for i, id := range d.ids {
			ids[i] = byte(id)
		}

		return &DictColumn{Entries: d.entries, IDs: ids, IDWidth: 1}
	}

	ids := make([]byte, len(d.ids)*2)
	for i, id := range d.ids {
		ids[i*2], ids[i*2+1] = byte(uint16(id)>>8), byte(uint16(id))
	}

	return &DictColumn{Entries: d.entries, IDs: ids, IDWidth: 2}
}

// putFlat writes c's rows into the flat form, at the granule's own offset in scatter mode.
func (d *DictMerger) putFlat(c *DictColumn, rows int) {
	if d.scatter {
		d.grow(d.at, rows)

		for i := range rows {
			d.flat[d.at+i] = c.At(i)
		}

		return
	}

	for i := range rows {
		d.flat = append(d.flat, c.At(i))
	}
}

// grow extends the accumulating arrays so a granule can be written at row offset off, filling any
// gap with the sentinel entry (id 0, the empty value reserved at Scatter time).
func (d *DictMerger) grow(off, n int) {
	if d.isFlat {
		for len(d.flat) < off+n {
			d.flat = append(d.flat, nil)
		}

		return
	}

	for len(d.ids) < off+n {
		d.ids = append(d.ids, 0)
	}
}

// degrade switches the merge to the flat form, materializing what it has accumulated so far.
func (d *DictMerger) degrade() {
	if d.isFlat {
		return
	}

	d.isFlat = true

	for _, id := range d.ids {
		d.flat = append(d.flat, d.entries[id])
	}

	d.ids = d.ids[:0]

	if d.m != nil {
		d.m.PutBack()
		d.m = nil
	}
}

// localID returns row r's dictionary id within c. c is dictionary-encoded (IDWidth > 0).
func (c *DictColumn) localID(r int) int {
	if c.IDWidth == 1 {
		return int(c.IDs[r])
	}

	return int(uint16(c.IDs[r*2])<<8 | uint16(c.IDs[r*2+1]))
}

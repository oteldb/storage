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

	// A column-wide dictionary registered once by [DictMerger.SeedShared], so the granules encoded
	// against it append through sharedRemap instead of re-merging its entries. sharedEntries is kept
	// for the flat form, which needs the values themselves rather than ids.
	sharedEntries [][]byte
	sharedRemap   []int32
	seeded        bool

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
// granule covers hold an unspecified value — the caller reads only the rows it selected.
//
// This is what lets a pruned read keep working in *part row indices*. A packed result would
// renumber every row a query already located through the part's row-range index and its marks, so
// every caller would have to translate; the int64 path avoids that by decoding into the destination
// at absolute offsets, and this gives bytes columns the same property. The cost is an id array
// sized to the column rather than to the selection — one or two bytes per skipped row, against the
// values those rows would otherwise have decoded.
func (d *DictMerger) Scatter(rows int) {
	d.scatter, d.rows = true, rows

	// Sized up front: grow would otherwise extend one row at a time, reallocating its way to the
	// column's length on the first granule that lands past the start.
	if cap(d.ids) < rows {
		d.ids = make([]int32, 0, rows)
	}
}

// Reset returns m to its empty state, keeping its buffers for reuse.
func (d *DictMerger) Reset() {
	if d.m != nil {
		d.m.PutBack()
		d.m = nil
	}

	d.entries, d.ids, d.flat, d.isFlat = d.entries[:0], d.ids[:0], d.flat[:0], false
	d.scatter, d.rows, d.at = false, 0, 0
	d.sharedEntries, d.sharedRemap, d.seeded = nil, d.sharedRemap[:0], false
}

// SeedShared registers a column-wide dictionary, remapping its entries into the merge once so that
// every granule encoded against it costs an id lookup per row and no hashing at all. Idempotent:
// only the first call for a merge does the work.
//
// This is what keeps a column that mixes both granule modes off a cliff. Without it, a granule
// carrying the shared dictionary as its own reaches [DictMerger.Append], which hashes every entry of
// it — so a column with one self-encoded granule paid granules x sharedEntries probes (measured:
// 2.4M for a 40-granule, 60509-entry attributes column, against zero for the same column with no
// self-encoded granule).
//
// Call it before appending the first shared granule, not at the start of the merge: seeding assigns
// ids, so seeding a selection that turns out to hold no shared granule would put unreferenced entries
// in the merged dictionary.
func (d *DictMerger) SeedShared(entries [][]byte) {
	if d.seeded {
		return
	}

	d.seeded, d.sharedEntries = true, entries

	if d.isFlat {
		return // the flat form reads sharedEntries directly; no ids to assign
	}

	d.initMap()

	if cap(d.sharedRemap) < len(entries) {
		d.sharedRemap = make([]int32, len(entries))
	}

	d.sharedRemap = d.sharedRemap[:len(entries)]

	for i, e := range entries {
		id, ok := d.m.Get(e)
		if !ok {
			// Same ceiling as [DictMerger.Append]: past it the merge cannot address an entry, so it
			// degrades and the shared granules materialize their values like any flat source.
			if len(d.entries) >= maxDictEntries {
				d.degrade()

				return
			}

			id = len(d.entries)
			d.m.Put(e, id)
			d.entries = append(d.entries, e)
		}

		d.sharedRemap[i] = int32(id)
	}
}

// AppendShared adds rows of a granule encoded as ids into the dictionary given to
// [DictMerger.SeedShared]. ids is the granule's packed big-endian id array at idWidth bytes per row;
// every id must be within the seeded dictionary, which the caller validates.
func (d *DictMerger) AppendShared(ids []byte, idWidth, rows int) {
	d.appendShared(ids, idWidth, rows, -1)
}

// AppendSharedAt is [DictMerger.AppendShared] landing at row offset off. Scatter mode only.
func (d *DictMerger) AppendSharedAt(ids []byte, idWidth, rows, off int) {
	d.appendShared(ids, idWidth, rows, off)
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

	// A granule whose dictionary holds one entry per row has no internal repeats, so merging it can
	// only pay if its values repeat in *other* granules — and with no seeded dictionary to repeat
	// into, they cannot: probing every value would build a dictionary the size of the column and then
	// degrade anyway, which is what a near-unique column (a log body) did on every whole-column
	// decode.
	//
	// A seeded merge is the opposite case. Its other granules index a column-wide dictionary, so
	// flattening for the sake of one unique granule discards *their* ids too — every row of the column
	// loses the per-distinct-entry predicate memo because one granule in forty had no repeats. There
	// the unique granule's entries are merged like any others and the ceiling below decides, which it
	// can do exactly.
	if c.IDWidth > 0 && len(c.Entries) == rows && !d.seeded {
		d.degrade()
	}

	if d.isFlat {
		d.putFlat(c, rows)

		return
	}

	// Decided before the entry loop rather than inside it: a granule that cannot fit whatever room is
	// left would otherwise be hashed entry by entry until it tripped the same ceiling, and every probe
	// up to that point is discarded by the degrade. Both counts are known here.
	if len(d.entries)+len(c.Entries) > maxDictEntries {
		d.degrade()
		d.putFlat(c, rows)

		return
	}

	// Scatter mode reserves id 0 for the rows no granule covers, so a skipped row decodes to an empty
	// value rather than indexing past the dictionary.
	d.initMap()

	// Remap the granule's local ids to global ones once per *entry*, not per row: a granule's rows
	// index a dictionary of at most 65536 entries, so the per-row loop below is an array lookup.
	local := make([]int32, len(c.Entries))

	// No ceiling check in the loop: the guard above admitted the granule's whole entry table, so the
	// dictionary cannot overflow inside it.
	for i, e := range c.Entries {
		id, ok := d.m.Get(e)
		if !ok {
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

func (d *DictMerger) appendShared(ids []byte, idWidth, rows, off int) {
	if rows == 0 {
		return
	}

	if d.isFlat {
		if d.scatter {
			d.grow(off, rows)

			for r := range rows {
				d.flat[off+r] = d.sharedEntries[sharedID(ids, idWidth, r)]
			}

			return
		}

		for r := range rows {
			d.flat = append(d.flat, d.sharedEntries[sharedID(ids, idWidth, r)])
		}

		return
	}

	if d.scatter {
		d.grow(off, rows)

		for r := range rows {
			d.ids[off+r] = d.sharedRemap[sharedID(ids, idWidth, r)]
		}

		return
	}

	for r := range rows {
		d.ids = append(d.ids, d.sharedRemap[sharedID(ids, idWidth, r)])
	}
}

// sharedID reads row r out of a packed big-endian id array.
func sharedID(ids []byte, idWidth, r int) int {
	if idWidth == 1 {
		return int(ids[r])
	}

	return int(uint16(ids[r*2])<<8 | uint16(ids[r*2+1]))
}

// initMap arms the entry map, reserving scatter mode's sentinel id 0 (see [DictMerger.Append]).
func (d *DictMerger) initMap() {
	if d.m != nil {
		return
	}

	d.m = pool.NewByteIntMap()

	if d.scatter && len(d.entries) == 0 {
		d.entries = append(d.entries, nil)
		d.m.Put(nil, 0)
	}
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
		if need := off + n; need > len(d.flat) {
			d.flat = append(d.flat, make([][]byte, need-len(d.flat))...)
		}

		return
	}

	if need := off + n; need > len(d.ids) {
		d.ids = append(d.ids, make([]int32, need-len(d.ids))...)
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

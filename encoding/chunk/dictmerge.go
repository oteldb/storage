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
}

// maxDictEntries is the largest dictionary a 2-byte row id can address; past it the merged column
// degrades to the flat form, as the single-granule encoder does.
const maxDictEntries = 1 << 16

// Reset returns m to its empty state, keeping its buffers for reuse.
func (d *DictMerger) Reset() {
	if d.m != nil {
		d.m.PutBack()
		d.m = nil
	}

	d.entries, d.ids, d.flat, d.isFlat = d.entries[:0], d.ids[:0], d.flat[:0], false
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
		for i := range rows {
			d.flat = append(d.flat, c.At(i))
		}

		return
	}

	if d.m == nil {
		d.m = pool.NewByteIntMap()
	}

	// Remap the granule's local ids to global ones once per *entry*, not per row: a granule's rows
	// index a dictionary of at most 65536 entries, so the per-row loop below is an array lookup.
	local := make([]int32, len(c.Entries))

	for i, e := range c.Entries {
		id, ok := d.m.Get(e)
		if !ok {
			if len(d.entries) >= maxDictEntries {
				d.degrade()

				for r := range rows {
					d.flat = append(d.flat, c.At(r))
				}

				return
			}

			id = len(d.entries)
			d.m.Put(e, id)
			d.entries = append(d.entries, e)
		}

		local[i] = int32(id)
	}

	switch c.IDWidth {
	case 1:
		for _, b := range c.IDs {
			d.ids = append(d.ids, local[b])
		}
	default:
		for r := range rows {
			d.ids = append(d.ids, local[(uint16(c.IDs[r*2])<<8)|uint16(c.IDs[r*2+1])])
		}
	}
}

// Build returns the merged column. Its byte slices alias those of the appended granules.
func (d *DictMerger) Build() *DictColumn {
	if d.m != nil {
		d.m.PutBack()
		d.m = nil
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

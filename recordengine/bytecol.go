package recordengine

// byteCol is one byte column of a [recordCols], laid out as a contiguous blob plus row end-offsets
// (arrow-style) rather than a [][]byte of per-cell slices. For an N-row column the GC scans two
// slice headers instead of N, and a per-row scan reads cells sequentially out of one allocation
// instead of chasing a pointer per row.
//
// offsets has len == rows+1; cell i is data[offsets[i]:offsets[i+1]]. offsets[0] is 0 for a column
// that owns its blob, and non-zero for a read-only row-range view of a larger one ([flushColumns.slice]
// takes one per output part of a split flush): every reader indexes the blob absolutely, so both
// forms are valid inputs to the part encoder. Only the appending paths assume a 0 origin. A cell
// view returned by [byteCol.at] aliases data and is invalidated by any append that grows or
// reallocates data — the same read-only-until-next-append rule the rest of the engine relies on; a
// caller that retains a value past an append must copy.
//
// int32 offsets cap a column blob at 2 GiB, which is ample for a head buffer bounded by the flush
// size and half the footprint of int64 offsets; a blob that would overflow simply forces a flush.
//
// # Interned form
//
// Repeated values are common in a head buffer: a part is sorted (stream, ts), and columns like a
// stream's serialized resource attributes or a severity text take a handful of distinct values over
// hundreds of thousands of rows. Such a column is held *interned* — data/offsets hold the distinct
// values and ids holds one 4-byte dictionary id per row — so it costs its distinct bytes plus 4 B a
// row rather than a full copy per row. Measured on real log parts: 149 MB → 108 KB for resource,
// 1.5 MB → 134 B for severity_text.
//
// A column whose values are near-unique (a span id, a trace id, a verbose log body) would pay a hash
// per row and the id index for nothing, so interning is abandoned once the distinct bytes exceed
// half the logical bytes — checked every internCheck rows, and permanently: a column that bails
// never interns again. Bailing [byteCol.expand]s it into the general layout.
//
// [byteCol.byteSize] reports the logical bytes in either form, so accounting is unaffected; the
// paths that index offsets per row — the part encoder, a row-range view — expand first.
type byteCol struct {
	data    []byte
	offsets []int32
	// interned puts the column in the interned form: ids holds one dictionary id per row, and
	// data/offsets hold the distinct values instead of the per-row cells.
	interned bool
	ids      []int32
	dict     map[string]int32
	// logical is the sum of the appended cells' lengths — what a flat layout would hold, and what
	// [byteCol.byteSize] reports. noIntern marks a column that has bailed out for good.
	logical  int64
	noIntern bool
}

const (
	// internCheck is how often (in rows) an interned column re-tests whether it is still paying off.
	// A column can start out repetitive and diverge later, so the test cannot happen only once.
	internCheck = 512
	// internMaxDict bounds the dictionary of a column that stays under the byte threshold but has
	// many distinct values, so the map cannot grow without limit.
	internMaxDict = 1 << 16
)

// rows returns the number of cells in the column. A zero-value (unselected) column has no offsets
// and reports 0.
func (b *byteCol) rows() int {
	if b.interned {
		return len(b.ids)
	}

	if len(b.offsets) == 0 {
		return 0
	}

	return len(b.offsets) - 1
}

// at returns a zero-copy view of cell i into the blob. Valid only until the next append.
func (b *byteCol) at(i int) []byte {
	if b.interned {
		id := b.ids[i]

		return b.data[b.offsets[id]:b.offsets[id+1]]
	}

	return b.data[b.offsets[i]:b.offsets[i+1]]
}

// expand rewrites an interned column into the general blob+offsets layout, for the paths that index
// offsets per row. A column already in that layout is untouched.
func (b *byteCol) expand() {
	if !b.interned {
		return
	}

	ids, dict, offsets := b.ids, b.data, b.offsets

	// The dictionary blob is the source, so the expansion builds into fresh storage — sized exactly,
	// since the logical length is already known.
	data := make([]byte, 0, b.logical)
	out := make([]int32, 1, len(ids)+1)

	for _, id := range ids {
		data = append(data, dict[offsets[id]:offsets[id+1]]...)
		out = append(out, int32(len(data)))
	}

	b.data, b.offsets, b.ids, b.dict, b.interned = data, out, b.ids[:0], nil, false
}

// ensure re-arms the column for a fresh accumulation of up to n cells, reusing the backing arrays
// when their capacity suffices (so a steady same-shape loop reallocates nothing). It sizes the row
// index only — see [byteCol.ensureBytes] for the callers that also know how many bytes are coming.
func (b *byteCol) ensure(n int) { b.ensureBytes(n, 0) }

// ensureBytes is [byteCol.ensure] with a blob capacity hint. Without one the blob grows by doubling
// from whatever capacity it had, so a fresh accumulation re-copies its own bytes ~log₂(size) times
// and overshoots its final capacity by up to 2× — CPU on the flush/merge hot path, and transient
// memory on exactly the columns (a part's worth of log bodies) that make it hurt. Every bulk caller
// knows the byte count: a dictionary's blob length, a source column's blob length, or the head's
// tracked per-stream byteSize.
func (b *byteCol) ensureBytes(n, blob int) {
	b.ids, b.dict, b.logical, b.interned, b.noIntern = b.ids[:0], nil, 0, false, false

	if cap(b.offsets) >= n+1 {
		b.offsets = b.offsets[:1]
		b.offsets[0] = 0
	} else {
		b.offsets = make([]int32, 1, n+1)
	}

	if blob > cap(b.data) {
		// Grow to at least the hint, but never by less than doubling: consecutive flushes differ in
		// size by a few percent, and an exact-fit allocation would be a few bytes short next time,
		// reallocating (and re-zeroing) the whole blob on every single flush.
		b.data = make([]byte, 0, max(blob, 2*cap(b.data)))

		return
	}

	b.data = b.data[:0]
}

// appendCell appends one cell, copying its bytes into the column (so the column owns them). It
// self-initializes the leading 0 offset, so a zero-value column may be appended to directly.
//
// An empty column starts out interned and stays that way until its distinct bytes outgrow the
// threshold — see [byteCol].
func (b *byteCol) appendCell(cell []byte) {
	if !b.interned && !b.noIntern && b.rows() == 0 {
		b.interned = true
		b.dict = make(map[string]int32)
		b.ids = b.ids[:0]
		b.offsets = append(b.offsets[:0], 0)
		b.data = b.data[:0]
	}

	if b.interned {
		b.appendInterned(cell)

		return
	}

	if len(b.offsets) == 0 {
		b.offsets = append(b.offsets, 0)
	}

	b.data = append(b.data, cell...)
	b.offsets = append(b.offsets, int32(len(b.data)))
	b.logical += int64(len(cell))
}

// appendInterned appends one cell to an interned column, adding it to the dictionary if new, and
// bails out of the form when it stops paying for itself.
func (b *byteCol) appendInterned(cell []byte) {
	if b.dict == nil { // a read-only view (see flushColumns.slice) cannot take appends interned
		b.expand()
		b.appendCell(cell)

		return
	}

	id, ok := b.dict[string(cell)]
	if !ok {
		id = int32(len(b.offsets) - 1)
		b.data = append(b.data, cell...)
		b.offsets = append(b.offsets, int32(len(b.data)))
		b.dict[string(cell)] = id
	}

	b.ids = append(b.ids, id)
	b.logical += int64(len(cell))

	if len(b.ids)%internCheck == 0 && !b.internPaysOff() {
		b.expand()
		b.noIntern = true
	}
}

// internPaysOff reports whether the dictionary is still smaller than half of what a flat layout
// would hold. Half, not equal, because interning also costs 4 bytes a row for the id: below that
// ratio the saving covers the index with room to spare, and above it a near-unique column (a span
// id, a trace id, a verbose body) would be paying a hash per row to store the same bytes twice.
func (b *byteCol) internPaysOff() bool {
	return len(b.dict) <= internMaxDict && int64(len(b.data))*2 <= b.logical
}

// appendRange bulk-appends cells [lo, hi) of src in one blob copy plus a rebased offset per cell.
// Either side being interned falls back to a cell-at-a-time append, which re-interns into b.
func (b *byteCol) appendRange(src *byteCol, lo, hi int) {
	if b.interned || src.interned {
		for i := lo; i < hi; i++ {
			b.appendCell(src.at(i))
		}

		return
	}

	if len(b.offsets) == 0 {
		b.offsets = append(b.offsets, 0)
	}

	base := src.offsets[lo]
	cur := int32(len(b.data))
	b.data = append(b.data, src.data[base:src.offsets[hi]]...)
	b.logical += int64(src.offsets[hi] - base)

	for i := lo; i < hi; i++ {
		b.offsets = append(b.offsets, cur+(src.offsets[i+1]-base))
	}
}

// keep retains only cells [lo, hi), reslicing the backing arrays in place (no copy) and rebasing the
// kept offsets to the new blob origin. Used by the fetch limit pushdown.
func (b *byteCol) keep(lo, hi int) {
	if b.interned {
		b.ids = b.ids[lo:hi]
		b.recountLogical()

		return
	}

	base := b.offsets[lo]
	b.data = b.data[base:b.offsets[hi]]

	off := b.offsets[lo : hi+1]
	for i := range off {
		off[i] -= base
	}

	b.offsets = off
}

// gather compacts the column to the cells named by idx (strictly increasing), rewriting the blob in
// place. Forward-safe: the cumulative kept bytes at any step never exceed the source cell's start,
// so the in-place copy never overwrites a cell it has yet to read.
func (b *byteCol) gather(idx []int) {
	if b.interned {
		for p, i := range idx {
			b.ids[p] = b.ids[i]
		}

		b.ids = b.ids[:len(idx)]
		b.recountLogical()

		return
	}

	var wb int32

	for p, i := range idx {
		lo, hi := b.offsets[i], b.offsets[i+1]
		l := hi - lo
		if wb != lo {
			copy(b.data[wb:wb+l], b.data[lo:hi])
		}

		b.offsets[p+1] = wb + l
		wb += l
	}

	b.data = b.data[:wb]
	b.offsets = b.offsets[:len(idx)+1]
}

// recountLogical re-derives the logical byte count after rows are dropped from an interned column.
func (b *byteCol) recountLogical() {
	b.logical = 0
	for _, id := range b.ids {
		b.logical += int64(b.offsets[id+1] - b.offsets[id])
	}
}

// byteSize returns the logical value bytes the column holds — what a reader materializes, and the
// basis for the head's in-flight accounting (the offset index is excluded).
func (b *byteCol) byteSize() int64 { return b.logical }

// storedSize returns the bytes actually occupied, which interning makes smaller than
// [byteCol.byteSize].
func (b *byteCol) storedSize() int64 {
	size := int64(len(b.data))
	if b.interned {
		size += 4 * int64(len(b.ids))
	}

	return size
}

// views fills dst with a zero-copy view per cell and returns it (growing dst as needed). The views
// alias the blob and share its lifetime; used only to materialize the [fetch.NamedColumn] boundary,
// where the producing accumulator outlives the batch via the recycle contract.
func (b *byteCol) views(dst [][]byte) [][]byte {
	n := b.rows()
	if cap(dst) < n {
		dst = make([][]byte, n)
	} else {
		dst = dst[:n]
	}

	for i := range n {
		dst[i] = b.at(i)
	}

	return dst
}

// permuteBytesInto writes src's cells reordered by idx into dst, reusing dst's backing arrays.
// An interned src permutes its id index only, sharing the dictionary.
func permuteBytesInto(dst, src *byteCol, idx []int) {
	if src.interned {
		ids := dst.ids[:0]
		if cap(ids) < len(idx) {
			ids = make([]int32, 0, len(idx))
		}

		for _, j := range idx {
			ids = append(ids, src.ids[j])
		}

		dst.data = append(dst.data[:0], src.data...)
		dst.offsets = append(dst.offsets[:0], src.offsets...)
		dst.ids, dst.dict, dst.logical, dst.interned, dst.noIntern = ids, nil, src.logical, true, src.noIntern

		return
	}

	// Size to the source exactly: dst is a scratch shared across columns of differing widths, where
	// ensureBytes' never-less-than-doubling rule would compound into a multiple of the largest.
	dst.ids, dst.dict, dst.logical, dst.interned, dst.noIntern = dst.ids[:0], nil, 0, false, true

	if cap(dst.data) < len(src.data) {
		dst.data = make([]byte, 0, len(src.data))
	} else {
		dst.data = dst.data[:0]
	}

	if cap(dst.offsets) < len(idx)+1 {
		dst.offsets = make([]int32, 0, len(idx)+1)
	} else {
		dst.offsets = dst.offsets[:0]
	}

	for _, j := range idx {
		dst.appendCell(src.at(j))
	}
}

// permuteBytes is [permuteBytesInto] without a reusable destination.
func permuteBytes(src *byteCol, idx []int) byteCol {
	out := byteCol{
		data:    make([]byte, 0, len(src.data)),
		offsets: make([]int32, 1, len(idx)+1),
	}

	for _, j := range idx {
		out.appendCell(src.at(j))
	}

	return out
}

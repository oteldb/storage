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
type byteCol struct {
	data    []byte
	offsets []int32
}

// rows returns the number of cells in the column. A zero-value (unselected) column has no offsets
// and reports 0.
func (b *byteCol) rows() int {
	if len(b.offsets) == 0 {
		return 0
	}

	return len(b.offsets) - 1
}

// at returns a zero-copy view of cell i into the blob. Valid only until the next append.
func (b *byteCol) at(i int) []byte { return b.data[b.offsets[i]:b.offsets[i+1]] }

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
func (b *byteCol) ensureBytes(n, bytes int) {
	if cap(b.offsets) >= n+1 {
		b.offsets = b.offsets[:1]
		b.offsets[0] = 0
	} else {
		b.offsets = make([]int32, 1, n+1)
	}

	if bytes > cap(b.data) {
		// Grow to at least the hint, but never by less than doubling: consecutive flushes differ in
		// size by a few percent, and an exact-fit allocation would be a few bytes short next time,
		// reallocating (and re-zeroing) the whole blob on every single flush.
		b.data = make([]byte, 0, max(bytes, 2*cap(b.data)))

		return
	}

	b.data = b.data[:0]
}

// appendCell appends one cell, copying its bytes into the blob (so the column owns them). It
// self-initializes the leading 0 offset, so a zero-value column may be appended to directly.
func (b *byteCol) appendCell(cell []byte) {
	if len(b.offsets) == 0 {
		b.offsets = append(b.offsets, 0)
	}

	b.data = append(b.data, cell...)
	b.offsets = append(b.offsets, int32(len(b.data)))
}

// appendRange bulk-appends cells [lo, hi) of src in one blob copy plus a rebased offset per cell.
func (b *byteCol) appendRange(src *byteCol, lo, hi int) {
	if len(b.offsets) == 0 {
		b.offsets = append(b.offsets, 0)
	}

	base := src.offsets[lo]
	cur := int32(len(b.data))
	b.data = append(b.data, src.data[base:src.offsets[hi]]...)

	for i := lo; i < hi; i++ {
		b.offsets = append(b.offsets, cur+(src.offsets[i+1]-base))
	}
}

// keep retains only cells [lo, hi), reslicing the backing arrays in place (no copy) and rebasing the
// kept offsets to the new blob origin. Used by the fetch limit pushdown.
func (b *byteCol) keep(lo, hi int) {
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

// byteSize returns the value bytes the column holds (the basis for the head's in-flight accounting,
// excluding the offset index).
func (b *byteCol) byteSize() int64 { return int64(len(b.data)) }

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

// permuteBytes returns a new column holding src's cells reordered by idx (used by the ts sort).
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

// permuteBytesInto writes src's cells reordered by idx into dst, reusing dst's backing arrays. It is
// [permuteBytes] with a caller-supplied destination (see [recordCols.sortByTsWith]). dst is sized to
// src exactly — it is a scratch shared across columns of differing widths, where a doubling growth
// rule would compound into a multiple of the largest.
func permuteBytesInto(dst, src *byteCol, idx []int) {
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

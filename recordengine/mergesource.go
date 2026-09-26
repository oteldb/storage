package recordengine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// defaultMergeReadWindow is how much of each source column a merge reads ahead per request, and so
// the read side's resident term: sources × columns × window, whatever the sources' size.
const defaultMergeReadWindow = 1 << 20

// mergeSource is one source part as a merge reads it, stream by stream in ascending id order.
type mergeSource interface {
	// appendStream appends the part's rows of stream id whose timestamps lie in [start, end] to acc.
	appendStream(acc *recordCols, id signal.SeriesID, start, end int64) error
}

// mergeReadObserver, when non-nil, receives per source of each merge whether it was read through a
// [partCursor] rather than decoded whole. Test seam only (see export_test.go).
var mergeReadObserver func(streamed []bool)

// mergeSkipStream, when non-nil, makes the stream sweep pass over the streams it reports: the sweep
// losing a stream, which [checkDrained] must turn into a failed merge. Test seam only.
var mergeSkipStream func(id signal.SeriesID) bool

// mergeReadWhole forces every source onto the whole decode, the oracle a [partCursor] merge is
// compared against. Test seam only; never set outside tests.
var mergeReadWhole = false

// openMergeSources opens a reader over every source part. A part whose streams occupy ascending row
// ranges is read forward, a granule at a time per column; any other is decoded whole.
//
// Byte columns open column by column across the sources, so a column whose sources are all read
// whole has its union resolved and its lookup index handed back before the next column builds one.
func (e *Engine) openMergeSources(ctx context.Context, src []*part, carry *mergeCarry) ([]mergeSource, error) {
	var (
		out      = make([]mergeSource, len(src))
		cursors  = make([]*partCursor, len(src))
		wholes   = make([]*wholeSource, len(src))
		streamed = make([]bool, len(src))
	)

	for i, p := range src {
		disorder := e.disorderReporter(ctx, p)

		forward := forwardReadable(p.ranges)

		if forward && !mergeReadWhole {
			c, err := openPartCursor(ctx, p, e.mergeReadWindow, carry, disorder)
			if err != nil {
				return nil, errors.Wrapf(err, "open part %q for merge", p.prefix)
			}

			out[i], cursors[i], streamed[i] = c, c, true

			continue
		}

		if !forward {
			disorder("part stream column is not in stream order; merging it from a whole decode")
		}

		s, err := openWholeSource(ctx, p)
		if err != nil {
			return nil, errors.Wrapf(err, "decode part %q for merge", p.prefix)
		}

		if !s.d.tsSorted {
			disorder("part rows are not timestamp-ordered within a stream; merging it without the windowed search")
		}

		out[i], wholes[i] = s, s
	}

	for k := range carry.dicts {
		name := e.cfg.Schema.byteColumn(k).Name

		for i, p := range src {
			if c := cursors[i]; c != nil {
				if err := c.bytes[k].open(ctx, p.reader, name, e.mergeReadWindow, carry); err != nil {
					return nil, errors.Wrapf(err, "open part %q for merge", p.prefix)
				}

				continue
			}

			wholes[i].resolve(k, carry)
		}

		carry.settle(k)
	}

	if mergeReadObserver != nil {
		mergeReadObserver(streamed)
	}

	return out, nil
}

// disorderReporter returns the once-per-part report of a source whose rows are not in the order
// both writers leave them. Such a part still merges without losing a row, but its windowed reads are
// already unreliable.
func (e *Engine) disorderReporter(ctx context.Context, p *part) func(msg string) {
	var reported bool

	return func(msg string) {
		if reported {
			return
		}

		reported = true

		zctx.From(ctx).Warn(msg, zap.String("part", p.prefix))
		e.cfg.Obs.Corruption.Detected(ctx, "stream_order", obs.CorruptTolerated)
	}
}

// forwardReadable reports whether a forward cursor can serve the part's streams in id order: ids must
// be strictly ascending, since the cursor takes one range per stream, and each stream's rows must
// begin at or past where the previous stream's ended. [buildRanges] sorts the ranges of a part whose
// stream column arrived unsorted, and those then point backwards or repeat an id.
func forwardReadable(ranges []streamRange) bool {
	pos := 0

	for i, r := range ranges {
		if i > 0 && r.id.Compare(ranges[i-1].id) <= 0 {
			return false
		}

		if mergestream.CheckForward(pos, r.start, r.end) != nil {
			return false
		}

		pos = r.end
	}

	return true
}

// checkDrained fails a merge in which a forward cursor was left holding streams: the sweep never
// asked for them, so their rows are missing from the output, and a merge that lost rows must not
// commit.
func checkDrained(src []*part, sources []mergeSource) error {
	for i, s := range sources {
		c, ok := s.(*partCursor)
		if !ok || c.next == len(c.ranges) {
			continue
		}

		return errors.Wrapf(block.ErrCorrupt, "merge left part %q at stream %v, %d of %d streams unread",
			src[i].prefix, c.ranges[c.next].id, len(c.ranges)-c.next, len(c.ranges))
	}

	return nil
}

// mergeShape sizes a merge's output buffer from its sources' manifests, without reading a column:
// their total row count and, per byte column, the bytes its objects hold should it be carried flat.
// That is the decoded size of a raw column written uncompressed or of a near-unique dictionary
// column, the usual flat ones, and an undercount the blob grows past otherwise. capBytes (0 ⇒ no
// seal) scales both down to what one output part holds.
func mergeShape(schema *Schema, src []*part, capBytes int64) (rows int, blob []int) {
	blob = make([]int, schema.numBytes())

	var total int64

	for _, p := range src {
		n := p.reader.RowCount()
		rows += n
		total += p.sizeBytes()

		for k := range blob {
			desc, ok := p.reader.ColumnDescByName(schema.byteColumn(k).Name)
			switch {
			case !ok:
			case desc.Const:
				blob[k] += n * len(desc.ConstBytes)
			default:
				blob[k] += int(desc.Bytes)
			}
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

// wholeSource is a source part decoded whole ([part.readForMerge]), for the part a [partCursor]
// cannot walk forward.
type wholeSource struct {
	ranges []streamRange
	d      *decodedPart
}

func openWholeSource(ctx context.Context, p *part) (*wholeSource, error) {
	d, err := p.readForMerge(ctx)
	if err != nil {
		return nil, err
	}

	for _, r := range p.ranges {
		if r.end > len(d.ts) {
			return nil, errors.Wrapf(block.ErrCorrupt, "stream rows [%d,%d) past %d timestamps", r.start, r.end, len(d.ts))
		}
	}

	d.remap = make([][]int32, len(d.bytes))

	return &wholeSource{ranges: p.ranges, d: d}, nil
}

// resolve maps byte column k's dictionary into the merge's union, or falls the column back to the
// flat carry when the part decoded it without one.
func (s *wholeSource) resolve(k int, carry *mergeCarry) {
	u := carry.dicts[k]
	if u == nil || len(s.d.ts) == 0 {
		return
	}

	col := &s.d.bytes[k]
	if col.dict == nil {
		carry.flatten(k)

		return
	}

	s.d.remap[k] = u.remap(nil, col.dict.Entries, true)
	carry.grew(k)
}

// appendStream appends every run the part holds of stream id: a part whose stream column arrived
// unsorted can hold several, which [buildRanges] leaves adjacent.
func (s *wholeSource) appendStream(acc *recordCols, id signal.SeriesID, start, end int64) error {
	i, _ := slices.BinarySearchFunc(s.ranges, id, func(sr streamRange, target signal.SeriesID) int {
		return sr.id.Compare(target)
	})

	for ; i < len(s.ranges) && s.ranges[i].id == id; i++ {
		appendMergeWindow(acc, s.d, s.ranges[i].rowRange, start, end)
	}

	return nil
}

// partCursor reads one source part forward: every column decodes one granule at a time, reached
// through a read-ahead window ([block.PartReader.ColumnScan]), so the part costs the merge a window
// and a granule per column instead of its decoded columns.
//
// Stream ranges are consumed in id order, which [forwardReadable] checked is row order too. A
// granule is still addressed by row, so a range that did step back would re-read frames rather than
// return wrong rows.
type partCursor struct {
	ranges   []streamRange
	next     int
	ts       intCursor
	ints     []intCursor
	bytes    []byteCursor
	carry    *mergeCarry
	disorder func(msg string)

	tsBuf []int64
	keep  []bool
}

func openPartCursor(
	ctx context.Context, p *part, window int64, carry *mergeCarry, disorder func(string),
) (*partCursor, error) {
	c := &partCursor{
		ranges:   p.ranges,
		ints:     make([]intCursor, p.schema.numInts()),
		bytes:    make([]byteCursor, p.schema.numBytes()),
		carry:    carry,
		disorder: disorder,
	}

	if err := c.ts.open(ctx, p.reader, colTs, window); err != nil {
		return nil, err
	}

	for k := range c.ints {
		if err := c.ints[k].open(ctx, p.reader, p.schema.intColumn(k).Name, window); err != nil {
			return nil, err
		}
	}

	for k := range c.bytes {
		c.bytes[k].k = k
	}

	return c, nil
}

func (c *partCursor) appendStream(acc *recordCols, id signal.SeriesID, start, end int64) error {
	if c.next == len(c.ranges) || c.ranges[c.next].id != id {
		return nil
	}

	rng := c.ranges[c.next].rowRange
	c.next++

	var err error
	if c.tsBuf, err = c.ts.appendRange(c.tsBuf[:0], rng.start, rng.end, nil); err != nil {
		return errors.Wrapf(err, "column %q", colTs)
	}

	keep, kept := c.window(start, end)
	if kept == 0 {
		return nil
	}

	for i, t := range c.tsBuf {
		if len(keep) == 0 || keep[i] {
			acc.ts = append(acc.ts, t)
			acc.noteTS(t)
		}
	}

	for k := range c.ints {
		if acc.ints[k], err = c.ints[k].appendRange(acc.ints[k], rng.start, rng.end, keep); err != nil {
			return errors.Wrapf(err, "column %q", acc.schema.intColumn(k).Name)
		}
	}

	for k := range c.bytes {
		if err := c.bytes[k].appendRange(acc, c.carry, rng.start, rng.end, keep); err != nil {
			return errors.Wrapf(err, "column %q", acc.schema.byteColumn(k).Name)
		}
	}

	return nil
}

// window selects the decoded stream's rows with a timestamp in [start, end]: keep is nil when every
// row is selected, and kept counts the selection. The rows are filtered one by one rather than
// binary-searched, so a stream out of ts order loses nothing; it is reported.
func (c *partCursor) window(start, end int64) (keep []bool, kept int) {
	ts := c.tsBuf

	for i, t := range ts {
		if t >= start && t <= end {
			kept++
		}

		if i > 0 && t < ts[i-1] {
			c.disorder("part rows are not timestamp-ordered within a stream")
		}
	}

	if kept == len(ts) || kept == 0 {
		return nil, kept
	}

	keep = slices.Grow(c.keep[:0], len(ts))[:len(ts)]
	for i, t := range ts {
		keep[i] = t >= start && t <= end
	}

	c.keep = keep

	return keep, kept
}

// intCursor serves rows of one int64 column from its current granule. A constant column needs no
// read at all, and an unblocked one — a part written before columns were framed — is one granule
// spanning the part.
type intCursor struct {
	dec    *block.Decoder
	lo, hi int
	vals   []int64

	constant bool
	value    int64
}

func (c *intCursor) open(ctx context.Context, r *block.PartReader, name string, window int64) error {
	desc, ok := r.ColumnDescByName(name)
	if !ok {
		return errors.Errorf("no column %q", name)
	}

	if desc.Kind != block.KindInt64 {
		return errors.Errorf("column %q is %s, not int64", name, desc.Kind)
	}

	switch {
	case desc.Const:
		c.constant, c.value, c.hi = true, desc.ConstInt64, r.RowCount()
	case !desc.Blocked:
		col, err := r.Column(ctx, name)
		if err != nil {
			return err
		}

		if c.vals, err = col.Int64(nil); err != nil {
			return errors.Wrapf(err, "column %q", name)
		}

		c.hi = len(c.vals)
	default:
		d, err := r.ColumnScan(ctx, name, window)
		if err != nil {
			return errors.Wrapf(err, "scan column %q", name)
		}

		c.dec = d
	}

	return nil
}

// appendRange appends the values of rows [lo, hi) selected by keep (nil ⇒ all) to dst.
func (c *intCursor) appendRange(dst []int64, lo, hi int, keep []bool) ([]int64, error) {
	for row := lo; row < hi; {
		if row < c.lo || row >= c.hi {
			if err := c.load(row); err != nil {
				return dst, err
			}
		}

		end := min(hi, c.hi)

		switch {
		case c.constant:
			for i := row; i < end; i++ {
				if len(keep) == 0 || keep[i-lo] {
					dst = append(dst, c.value)
				}
			}
		case keep == nil:
			dst = append(dst, c.vals[row-c.lo:end-c.lo]...)
		default:
			for i, v := range c.vals[row-c.lo : end-c.lo] {
				if keep[row-lo+i] {
					dst = append(dst, v)
				}
			}
		}

		row = end
	}

	return dst, nil
}

func (c *intCursor) load(row int) error {
	lo, hi, err := granuleOf(c.dec, row)
	if err != nil {
		return err
	}

	blk := row / c.dec.BlockRows()

	vals, err := c.dec.DecodeInt64Into(blk, c.vals)
	if err != nil {
		return errors.Wrapf(err, "decode granule %d", blk)
	}

	if len(vals) != hi-lo {
		return errors.Wrapf(block.ErrCorrupt, "granule %d decoded %d rows, want %d", blk, len(vals), hi-lo)
	}

	c.vals, c.lo, c.hi = vals, lo, hi

	return nil
}

// granuleOf returns the row span of the granule holding row, failing for a row the column does not
// have — which, past a whole-column read, is a part whose columns disagree on its row count.
func granuleOf(d *block.Decoder, row int) (lo, hi int, _ error) {
	if d == nil || d.BlockRows() <= 0 || row/d.BlockRows() >= d.NumBlocks() {
		return 0, 0, errors.Wrapf(block.ErrCorrupt, "row %d past the column", row)
	}

	lo, hi = d.BlockSpan(row / d.BlockRows())

	return lo, hi, nil
}

// byteCursor serves rows of one bytes column from its current granule, either as cells or, for a
// column on the split carry, as ids into the merge's union — each granule's entries are remapped
// once, then every row is a table lookup.
//
// A column with no granules is read whole and resolved into the union as it opens. That is a
// current layout, not only a legacy one: a dictionary column none of whose granules joined a shared
// dictionary is written as one unframed stream.
type byteCursor struct {
	k      int
	dec    *block.Decoder
	lo, hi int
	col    mergeByteCol
	// granule holds the current granule's column, so the cursor's view of it needs no allocation.
	granule chunk.DictColumn

	constant bool
	value    []byte

	// shared is the column's shared dictionary and sharedRemap its union ids, resolved on the first
	// granule that uses it; selfRemap is the union ids of the current granule's own dictionary.
	shared      [][]byte
	sharedRemap []int32
	selfRemap   []int32
	remap       []int32
	remapped    bool
}

func (c *byteCursor) open(ctx context.Context, r *block.PartReader, name string, window int64, carry *mergeCarry) error {
	desc, ok := r.ColumnDescByName(name)
	if !ok {
		return errors.Errorf("no column %q", name)
	}

	if desc.Kind != block.KindBytes {
		return errors.Errorf("column %q is %s, not bytes", name, desc.Kind)
	}

	switch {
	case desc.Const:
		c.constant, c.value, c.hi = true, desc.ConstBytes, r.RowCount()
	case !desc.Blocked:
		col, err := r.Column(ctx, name)
		if err != nil {
			return err
		}

		dc, err := col.Bytes()
		if err != nil {
			return errors.Wrapf(err, "column %q", name)
		}

		c.col, c.hi = newMergeByteCol(dc), dc.Len()
	default:
		d, err := r.ColumnScan(ctx, name, window)
		if err != nil {
			return errors.Wrapf(err, "scan column %q", name)
		}

		c.dec = d
		c.shared, _ = d.SharedEntries()

		if carry.dicts[c.k] != nil {
			carry.lazy[c.k]++
		}

		return nil
	}

	if carry.dicts[c.k] != nil {
		c.remapGranule(carry)
	}

	return nil
}

// appendRange appends rows [lo, hi) selected by keep (nil ⇒ all) to byte column k of acc.
func (c *byteCursor) appendRange(acc *recordCols, carry *mergeCarry, lo, hi int, keep []bool) error {
	for row := lo; row < hi; {
		if row < c.lo || row >= c.hi {
			if err := c.load(row); err != nil {
				return err
			}
		}

		end := min(hi, c.hi)

		var mask []bool
		if keep != nil {
			mask = keep[row-lo : end-lo]
		}

		if acc.splitAt(c.k) != nil && !c.remapped {
			c.remapGranule(carry)
		}

		if sc := acc.splitAt(c.k); sc != nil {
			c.appendIDs(sc, row-c.lo, end-row, mask)
		} else {
			c.appendCells(&acc.bytes[c.k], row-c.lo, end-row, mask)
		}

		row = end
	}

	return nil
}

func (c *byteCursor) load(row int) error {
	lo, hi, err := granuleOf(c.dec, row)
	if err != nil {
		return err
	}

	blk := row / c.dec.BlockRows()

	g, err := c.dec.DecodeBytesBlock(blk)
	if err != nil {
		return err
	}

	c.granule = g.Column()
	c.col, c.lo, c.hi, c.remapped = mergeByteCol{dict: &c.granule}, lo, hi, false

	return nil
}

// remapGranule resolves the current granule's entries to union ids, falling the column back to the
// flat carry when the granule has no dictionary or the union outgrows its bound. A granule's entries
// alias the decoder's frame, which the next granule overwrites, so they are copied into the union; a
// whole-read column's and the shared dictionary's are the cursor's own and are kept as they are.
func (c *byteCursor) remapGranule(carry *mergeCarry) {
	c.remapped = true
	u := carry.dicts[c.k]
	dc := c.col.dict

	switch {
	case c.constant:
		c.selfRemap = append(c.selfRemap[:0], u.id(c.value))
		c.remap = c.selfRemap
	case dc == nil || (dc.IDWidth == 0 && dc.Len() > 0):
		carry.flatten(c.k)

		return
	case dc.Len() == 0:
		return
	case c.onShared():
		if c.sharedRemap == nil {
			c.sharedRemap = u.remap(nil, c.shared, true)
		}

		c.remap = c.sharedRemap
	case c.dec == nil:
		c.remap = u.remap(nil, dc.Entries, true)
	default:
		c.selfRemap = u.remap(c.selfRemap, dc.Entries, false)
		c.remap = c.selfRemap
	}

	carry.grew(c.k)
}

// onShared reports whether the current granule's ids index the column's shared dictionary, which
// [block.Decoder.DecodeBytesBlock] hands back as the granule's own entries.
func (c *byteCursor) onShared() bool {
	entries := c.col.dict.Entries

	return len(c.shared) > 0 && len(entries) > 0 && &entries[0] == &c.shared[0]
}

func (c *byteCursor) appendIDs(sc *splitCol, base, n int, mask []bool) {
	if c.constant {
		id := c.remap[0]
		for j := range n {
			if len(mask) == 0 || mask[j] {
				sc.append(id)
			}
		}

		return
	}

	for j := range n {
		if len(mask) == 0 || mask[j] {
			sc.append(c.remap[c.col.entryID(base+j)])
		}
	}
}

func (c *byteCursor) appendCells(bc *byteCol, base, n int, mask []bool) {
	for j := range n {
		if len(mask) != 0 && !mask[j] {
			continue
		}

		if c.constant {
			bc.appendCell(c.value)
		} else {
			bc.appendCell(c.col.at(base + j))
		}
	}
}

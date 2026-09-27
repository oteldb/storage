package recordengine

import (
	"context"
	"slices"
	"strconv"

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
	// run returns the part's rows of stream id with a timestamp at or after start, in the order the
	// merge writes them; ok is false when there are none.
	run(id signal.SeriesID, start int64) (r mergeRun, ok bool, err error)
	// residentBytes bounds what the source holds in RAM from the moment it opens: read-ahead windows,
	// frame buffers and dictionaries for a forward read, the decoded columns for a whole decode.
	residentBytes() int64
	// dictEntries is how many entries of stable dictionaries the source hands a writer, which a
	// writer binding every source caches an id for each of.
	dictEntries() int
}

// mergeRun is one source's rows of one stream, ordered by timestamp and then by their row in the
// part — the order of a stable sort by timestamp, which is what makes a k-way merge of runs equal
// to concatenating them in source order and sorting stably.
type mergeRun interface {
	// ahead returns the timestamps from the current row to the end of what the run holds decoded,
	// at most [mergeGranuleRows]; it is empty once the run is drained.
	ahead() ([]int64, error)
	// rowBytes is the decoded size of the i-th row ahead, stream id included.
	rowBytes(i int) int64
	// emit appends every column but the timestamp of the next n rows ahead to w, as source si, and
	// advances past them.
	emit(w *recordPartStreamWriter, si, n int) error
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
// ranges is read forward, a granule at a time per column; any other is decoded whole, as is one an
// earlier merge found out of timestamp order ([errRunDisorder]).
func (e *Engine) openMergeSources(ctx context.Context, src []*part) ([]mergeSource, error) {
	var (
		out      = make([]mergeSource, len(src))
		streamed = make([]bool, len(src))
	)

	for i, p := range src {
		disorder := e.disorderReporter(ctx, p)

		forward := forwardReadable(p.ranges)

		if forward && !mergeReadWhole && !p.tsDisorder.Load() {
			c, err := openPartCursor(ctx, p, e.mergeReadWindow)
			if err != nil {
				return nil, errors.Wrapf(err, "open part %q for merge", p.prefix)
			}

			out[i], streamed[i] = c, true

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

		out[i] = s
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

// fixedRowBytes is the decoded size of a row's fixed-width cells: the stream id, the timestamp and
// every int column.
func fixedRowBytes(schema *Schema) int64 {
	return streamIDBytes + 8 + 8*int64(schema.numInts())
}

// wholeSource is a source part decoded whole ([part.readForMerge]), for the part a [partCursor]
// cannot walk forward.
type wholeSource struct {
	schema *Schema
	ranges []streamRange
	d      *decodedPart
	served arrayRun
	gather gatherRun
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

	s := &wholeSource{schema: p.schema, ranges: p.ranges, d: d}
	s.served.init(p.schema)
	s.gather.init(p.schema)

	return s, nil
}

func (s *wholeSource) residentBytes() int64 { return s.d.residentBytes() }

func (s *wholeSource) dictEntries() int {
	var n int

	for k := range s.d.bytes {
		n += len(s.d.bytes[k].entries)
	}

	return n
}

// run serves one ts-ordered range straight from the decoded columns. A stream whose rows are out of
// order, or that the part holds in several runs — which [buildRanges] leaves adjacent — is gathered
// and sorted instead.
func (s *wholeSource) run(id signal.SeriesID, start int64) (mergeRun, bool, error) {
	lo, _ := slices.BinarySearchFunc(s.ranges, id, func(sr streamRange, target signal.SeriesID) int {
		return sr.id.Compare(target)
	})

	hi := lo
	for hi < len(s.ranges) && s.ranges[hi].id == id {
		hi++
	}

	switch {
	case lo == hi:
		return nil, false, nil
	case hi-lo == 1 && s.d.tsSorted:
		w := tsWindow(s.d.ts, s.ranges[lo].rowRange, start, maxInt64)
		if w.start == w.end {
			return nil, false, nil
		}

		s.served.over(s.d, w)

		return &s.served, true, nil
	}

	g := &s.gather
	g.reset(s.schema)

	for _, r := range s.ranges[lo:hi] {
		for row := r.start; row < r.end; row++ {
			if s.d.ts[row] >= start {
				g.appendDecoded(s.d, row)
			}
		}
	}

	r := g.sorted()

	return r, r != nil, nil
}

// arrayRun serves a run out of fully decoded columns: a whole-decoded part's range, or a gathered
// stream ([gatherRun]).
type arrayRun struct {
	fixed int64
	ts    []int64
	ints  [][]int64
	bytes []arrayBytes
	pos   int
}

// arrayBytes is one byte column of an [arrayRun]: base is the row of col the run's first row is at.
type arrayBytes struct {
	col     *chunk.DictColumn
	g       block.DecodedGranule
	entries [][]byte
	stable  bool
	base    int
}

func (r *arrayRun) init(schema *Schema) {
	r.fixed = fixedRowBytes(schema)
	r.ints = make([][]int64, schema.numInts())
	r.bytes = make([]arrayBytes, schema.numBytes())
}

// over points r at rows w of the decoded part d.
func (r *arrayRun) over(d *decodedPart, w rowRange) {
	r.ts, r.pos = d.ts[w.start:w.end], 0

	for k := range r.ints {
		r.ints[k] = d.ints[k][w.start:w.end]
	}

	for k := range r.bytes {
		r.bytes[k] = d.bytes[k]
		r.bytes[k].base = w.start
	}
}

func (r *arrayRun) ahead() ([]int64, error) {
	return r.ts[r.pos:min(len(r.ts), r.pos+mergeGranuleRows)], nil
}

func (r *arrayRun) rowBytes(i int) int64 {
	n := r.fixed

	for k := range r.bytes {
		b := &r.bytes[k]
		n += int64(len(b.col.At(b.base + r.pos + i)))
	}

	return n
}

func (r *arrayRun) emit(w *recordPartStreamWriter, si, n int) error {
	lo, hi := r.pos, r.pos+n

	for k := range r.ints {
		if err := w.appendInts(k, r.ints[k][lo:hi]); err != nil {
			return err
		}
	}

	for k := range r.bytes {
		b := &r.bytes[k]
		if err := w.appendBytes(si, k, b.g, b.entries, b.stable, b.base+lo, b.base+hi); err != nil {
			return err
		}
	}

	r.pos = hi

	return nil
}

// gatherRun holds one source's rows of a stream that cannot be served in place — out of timestamp
// order, or split over several runs of a part — copied out and stably sorted. It is O(the stream's
// rows in that source), which is why only a part both writers would never produce takes it.
type gatherRun struct {
	rows  *recordCols
	cols  []chunk.DictColumn
	views [][][]byte
	run   arrayRun
}

func (g *gatherRun) init(schema *Schema) {
	g.rows = newRecordCols(schema, 0, fullSel(schema))
	g.cols = make([]chunk.DictColumn, schema.numBytes())
	g.views = make([][][]byte, schema.numBytes())
	g.run.init(schema)
}

func (g *gatherRun) reset(schema *Schema) { g.rows.prepare(schema, 0, fullSel(schema)) }

func (g *gatherRun) appendDecoded(d *decodedPart, row int) {
	c := g.rows
	c.ts = append(c.ts, d.ts[row])

	for k := range c.ints {
		c.ints[k] = append(c.ints[k], d.ints[k][row])
	}

	for k := range c.bytes {
		c.bytes[k].appendCell(d.bytes[k].col.At(row))
	}
}

// sorted sorts the gathered rows by timestamp, stably, and returns them as a run, or nil when none
// was gathered. The run's values are views into the gathered blob, which the next gather reuses, so
// they are bound as a table the writer must copy.
func (g *gatherRun) sorted() mergeRun {
	c := g.rows
	if c.len() == 0 {
		return nil
	}

	c.sortByTs()

	r := &g.run
	r.ts, r.pos = c.ts, 0

	for k := range r.ints {
		r.ints[k] = c.ints[k]
	}

	for k := range r.bytes {
		g.views[k] = c.bytes[k].views(g.views[k])
		g.cols[k] = chunk.DictColumn{Entries: g.views[k]}
		r.bytes[k] = arrayBytes{col: &g.cols[k], g: block.OwnedGranule(&g.cols[k], block.NewDictGen())}
	}

	return r
}

// partCursor reads one source part forward: every column decodes one granule at a time, reached
// through a read-ahead window ([block.PartReader.ColumnScan]), so the part costs the merge a window
// and a granule per column instead of its decoded columns.
//
// Stream ranges are consumed in id order, which [forwardReadable] checked is row order too. A
// granule is still addressed by row, so a range that did step back would re-read frames rather than
// return wrong rows.
//
// A run is served in place on the premise that its timestamps ascend, which both writers guarantee
// and nothing checks when the part opens. Every row a run passes over is checked against it, and a
// run that breaks it fails the merge with [errRunDisorder] — rows already written in the wrong order
// cannot be taken back.
type partCursor struct {
	ranges []streamRange
	next   int
	fixed  int64

	ts    intCursor
	ints  []intCursor
	bytes []byteCursor

	pos, end int
	last     int64

	resident int64
}

func openPartCursor(ctx context.Context, p *part, window int64) (*partCursor, error) {
	c := &partCursor{
		ranges: p.ranges,
		fixed:  fixedRowBytes(p.schema),
		ints:   make([]intCursor, p.schema.numInts()),
		bytes:  make([]byteCursor, p.schema.numBytes()),
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
		if err := c.bytes[k].open(ctx, p.reader, p.schema.byteColumn(k).Name, window); err != nil {
			return nil, err
		}
	}

	// A constant timestamp column is filled a whole stream at a time by [partCursor.run], every
	// other one a granule at a time.
	longest := 0
	for _, r := range p.ranges {
		longest = max(longest, r.end-r.start)
	}

	c.resident = c.ts.residentBytes(longest)
	for k := range c.ints {
		c.resident += c.ints[k].residentBytes(mergeGranuleRows)
	}

	for k := range c.bytes {
		c.resident += c.bytes[k].residentBytes()
	}

	return c, nil
}

func (c *partCursor) residentBytes() int64 { return c.resident }

func (c *partCursor) dictEntries() int {
	var n int

	for k := range c.bytes {
		b := &c.bytes[k]
		if b.stable {
			n += len(b.entries)
		}

		n += len(b.shared)
	}

	return n
}

// errRunDisorder reports a source whose stream was found out of timestamp order mid-merge.
var errRunDisorder = errors.New("part rows are not timestamp-ordered within a stream")

// sourceDisorderError names the source a merge found out of timestamp order, which the retry decodes
// whole.
type sourceDisorderError struct{ src int }

func (e *sourceDisorderError) Error() string {
	return errRunDisorder.Error() + " (source " + strconv.Itoa(e.src) + ")"
}

func (e *sourceDisorderError) Unwrap() error { return errRunDisorder }

// sourceErr attributes err to source si when it is a disorder, so the merge can retry around it.
func sourceErr(si int, err error) error {
	if errors.Is(err, errRunDisorder) {
		return &sourceDisorderError{src: si}
	}

	return err
}

func (c *partCursor) run(id signal.SeriesID, start int64) (mergeRun, bool, error) {
	if c.next == len(c.ranges) || c.ranges[c.next].id != id {
		return nil, false, nil
	}

	r := c.ranges[c.next]
	c.next++

	c.pos, c.end, c.last = r.start, r.end, minInt64

	for c.pos < c.end {
		ts, err := c.ts.from(c.pos, c.end)
		if err != nil {
			return nil, false, err
		}

		for _, t := range ts {
			if t < c.last {
				return nil, false, errRunDisorder
			}

			if t >= start {
				return c, true, nil
			}

			c.last = t
			c.pos++
		}
	}

	return nil, false, nil
}

// ahead loads the granule holding the current row in every column and returns the timestamps up to
// the nearest granule end, so every row it returns is decoded in every column.
func (c *partCursor) ahead() ([]int64, error) {
	if c.pos >= c.end {
		return nil, nil
	}

	ts, err := c.ts.from(c.pos, min(c.end, c.pos+mergeGranuleRows))
	if err != nil {
		return nil, errors.Wrapf(err, "column %q", colTs)
	}

	hi := c.pos + len(ts)

	for k := range c.ints {
		if err := c.ints[k].ensure(c.pos); err != nil {
			return nil, err
		}

		hi = min(hi, c.ints[k].hi)
	}

	for k := range c.bytes {
		if err := c.bytes[k].ensure(c.pos); err != nil {
			return nil, err
		}

		hi = min(hi, c.bytes[k].hi)
	}

	return ts[:hi-c.pos], nil
}

func (c *partCursor) rowBytes(i int) int64 {
	n, row := c.fixed, c.pos+i

	for k := range c.bytes {
		n += int64(len(c.bytes[k].at(row)))
	}

	return n
}

func (c *partCursor) emit(w *recordPartStreamWriter, si, n int) error {
	lo, hi := c.pos, c.pos+n

	for _, t := range c.ts.slice(lo, hi) {
		if t < c.last {
			return errRunDisorder
		}

		c.last = t
	}

	for k := range c.ints {
		if err := w.appendInts(k, c.ints[k].slice(lo, hi)); err != nil {
			return err
		}
	}

	for k := range c.bytes {
		if err := c.bytes[k].emit(w, si, k, lo, hi); err != nil {
			return err
		}
	}

	c.pos = hi

	return nil
}

// intCursor serves rows of one int64 column from its current granule. A constant column needs no
// read at all, and an unblocked one — a part written before columns were framed — is one granule
// spanning the part.
type intCursor struct {
	name   string
	dec    *block.Decoder
	lo, hi int
	vals   []int64

	constant bool
	value    int64
	fill     []int64
}

func (c *intCursor) open(ctx context.Context, r *block.PartReader, name string, window int64) error {
	desc, ok := r.ColumnDescByName(name)
	if !ok {
		return errors.Errorf("no column %q", name)
	}

	if desc.Kind != block.KindInt64 {
		return errors.Errorf("column %q is %s, not int64", name, desc.Kind)
	}

	c.name = name

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

// residentBytes bounds what the cursor holds: its decoder, the whole column when it was read whole,
// or for a constant column a fill of up to fillRows.
func (c *intCursor) residentBytes(fillRows int) int64 {
	switch {
	case c.constant:
		return int64(fillRows) * 8
	case c.dec != nil:
		return c.dec.ResidentBytes()
	default:
		return int64(cap(c.vals)) * 8
	}
}

// ensure makes row's granule the current one.
func (c *intCursor) ensure(row int) error {
	if row >= c.lo && row < c.hi {
		return nil
	}

	if c.dec == nil {
		return errors.Wrapf(block.ErrCorrupt, "column %q: row %d past %d rows", c.name, row, c.hi)
	}

	lo, hi, err := granuleOf(c.dec, row)
	if err != nil {
		return errors.Wrapf(err, "column %q", c.name)
	}

	blk := row / c.dec.BlockRows()

	vals, err := c.dec.DecodeInt64Into(blk, c.vals)
	if err != nil {
		return errors.Wrapf(err, "column %q: decode granule %d", c.name, blk)
	}

	if len(vals) != hi-lo {
		return errors.Wrapf(block.ErrCorrupt, "column %q: granule %d decoded %d rows, want %d", c.name, blk, len(vals), hi-lo)
	}

	c.vals, c.lo, c.hi = vals, lo, hi

	return nil
}

// from returns the values of rows [row, min(end, the granule's end)).
func (c *intCursor) from(row, end int) ([]int64, error) {
	if err := c.ensure(row); err != nil {
		return nil, err
	}

	return c.slice(row, min(end, c.hi)), nil
}

// slice returns the values of rows [lo, hi) of the current granule. A constant column's are filled
// into a buffer the next call reuses.
func (c *intCursor) slice(lo, hi int) []int64 {
	if !c.constant {
		return c.vals[lo-c.lo : hi-c.lo]
	}

	c.fill = slices.Grow(c.fill[:0], hi-lo)[:hi-lo]
	for i := range c.fill {
		c.fill[i] = c.value
	}

	return c.fill
}

func (c *intCursor) at(row int) int64 {
	if c.constant {
		return c.value
	}

	return c.vals[row-c.lo]
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

// byteCursor serves rows of one bytes column from its current granule, handed to the writer whole
// with the table its ids index, so a row costs the writer a cached id lookup rather than a hash.
//
// A column with no granules is read whole. That is a current layout, not only a legacy one: a
// dictionary column none of whose granules joined a shared dictionary is written as one unframed
// stream by the flush.
type byteCursor struct {
	name   string
	dec    *block.Decoder
	lo, hi int

	g   block.DecodedGranule
	col chunk.DictColumn
	// entries is the table the granule's ids index, nil for a granule with one entry per row; stable
	// says it outlives the cursor's next decode.
	entries [][]byte
	stable  bool

	shared    [][]byte
	sharedGen block.DictGen

	constant bool
	constIDs []byte
	constCol chunk.DictColumn
	constGen block.DictGen
}

func (c *byteCursor) open(ctx context.Context, r *block.PartReader, name string, window int64) error {
	desc, ok := r.ColumnDescByName(name)
	if !ok {
		return errors.Errorf("no column %q", name)
	}

	if desc.Kind != block.KindBytes {
		return errors.Errorf("column %q is %s, not bytes", name, desc.Kind)
	}

	c.name = name

	switch {
	case desc.Const:
		c.constant, c.hi = true, r.RowCount()
		c.constCol = chunk.DictColumn{Entries: [][]byte{desc.ConstBytes}, IDWidth: 1}
		c.constGen = block.NewDictGen()
	case !desc.Blocked:
		col, err := r.Column(ctx, name)
		if err != nil {
			return err
		}

		dc, err := col.Bytes()
		if err != nil {
			return errors.Wrapf(err, "column %q", name)
		}

		c.g, c.col, c.hi, c.stable = block.OwnedGranule(dc, block.NewDictGen()), *dc, dc.Len(), true
		if dc.IDWidth != 0 {
			c.entries = dc.Entries
		}
	default:
		d, err := r.ColumnScan(ctx, name, window)
		if err != nil {
			return errors.Wrapf(err, "scan column %q", name)
		}

		c.dec = d
		c.shared, c.sharedGen = d.SharedEntries()
	}

	return nil
}

// residentBytes bounds what the cursor holds: its decoder, the whole column when it was read whole,
// or for a constant column the ids of one granule.
func (c *byteCursor) residentBytes() int64 {
	switch {
	case c.constant:
		return mergeGranuleRows
	case c.dec != nil:
		return c.dec.ResidentBytes()
	default:
		return dictColumnBytes(&c.col)
	}
}

func (c *byteCursor) ensure(row int) error {
	if row >= c.lo && row < c.hi {
		return nil
	}

	if c.dec == nil {
		return errors.Wrapf(block.ErrCorrupt, "column %q: row %d past %d rows", c.name, row, c.hi)
	}

	lo, hi, err := granuleOf(c.dec, row)
	if err != nil {
		return errors.Wrapf(err, "column %q", c.name)
	}

	blk := row / c.dec.BlockRows()

	g, err := c.dec.DecodeBytesBlock(blk)
	if err != nil {
		return errors.Wrapf(err, "column %q", c.name)
	}

	c.g, c.col, c.lo, c.hi = g, g.Column(), lo, hi

	switch {
	case c.col.IDWidth == 0:
		c.entries, c.stable = nil, false
	case len(c.shared) > 0 && g.Table() == c.sharedGen:
		c.entries, c.stable = c.shared, true
	default:
		c.entries, c.stable = c.col.Entries, false
	}

	return nil
}

func (c *byteCursor) at(row int) []byte {
	if c.constant {
		return c.constCol.Entries[0]
	}

	return c.col.At(row - c.lo)
}

// emit appends rows [lo, hi) of the current granule to byte column k of w. A constant column is
// handed over as a one-entry table.
func (c *byteCursor) emit(w *recordPartStreamWriter, si, k, lo, hi int) error {
	if !c.constant {
		return w.appendBytes(si, k, c.g, c.entries, c.stable, lo-c.lo, hi-c.lo)
	}

	n := hi - lo
	if len(c.constIDs) < n {
		c.constIDs = make([]byte, n)
	}

	c.constCol.IDs = c.constIDs[:n]

	return w.appendBytes(si, k, block.OwnedGranule(&c.constCol, c.constGen), c.constCol.Entries, true, 0, n)
}

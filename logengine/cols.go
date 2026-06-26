package logengine

import (
	"bytes"
	"sort"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// rec is one log record's column values in the engine's internal, signal-neutral form: the
// numeric fields plus the byte fields (attrs already serialized via the reversible
// [signal.Attributes] encoding). It is the unit head buffers, flush columns, WAL records, and the
// log model all map to, so the head depends on neither signal/log nor wal.
type rec struct {
	ts       int64
	observed int64
	severity int64
	flags    int64
	dropped  int64
	sevText  []byte
	body     []byte
	traceID  []byte
	spanID   []byte
	attrs    []byte // serialized attributes (opaque here)
}

// colSet is the set of per-record columns a fetch must materialize — the timestamp is always
// needed, so it is not tracked here. A fetch decodes, copies, and outputs only these columns
// (lazy column decode): the conditions' columns (for filtering) ∪ the projection (for output).
type colSet struct {
	observed, severity, flags, dropped bool
	sevText, body, traceID, spanID     bool
	attrs                              bool
}

// allCols selects every column (the default when a request has no projection).
var allCols = colSet{true, true, true, true, true, true, true, true, true}

// mark sets the bit for a fixed column name (a no-op for the always-present ts or an unknown name).
func (s *colSet) mark(name string) {
	switch name {
	case colObserved:
		s.observed = true
	case colSeverity:
		s.severity = true
	case colFlags:
		s.flags = true
	case colDropped:
		s.dropped = true
	case colSevText:
		s.sevText = true
	case colBody:
		s.body = true
	case colTraceID:
		s.traceID = true
	case colSpanID:
		s.spanID = true
	case colAttrs:
		s.attrs = true
	}
}

// selectColumns derives the column set a request needs: an empty projection means "all columns"
// (output everything); otherwise the projected columns (output) plus the conditions' columns
// (filter) — an attribute condition needs the serialized attrs blob.
func selectColumns(r fetch.Request) colSet {
	if len(r.Projection) == 0 {
		return allCols
	}

	var s colSet
	for _, name := range r.Projection {
		s.mark(name)
	}

	for i := range r.Conditions {
		if isFixedColumn(r.Conditions[i].Column) {
			s.mark(r.Conditions[i].Column)
		} else {
			s.attrs = true // a per-record attribute predicate reads the blob
		}
	}

	return s
}

// recordCols is a set of log records laid out columnarly (one entry per record across the parallel
// slices). It is the head's per-stream buffer, the per-stream slice of a flush, and a fetch
// accumulator. The ts slice is always populated; the other columns are populated only when sel
// selects them (lazy decode). All populated slices share one length, [recordCols.len].
type recordCols struct {
	sel      colSet
	ts       []int64
	observed []int64
	severity []int64
	flags    []int64
	dropped  []int64
	sevText  [][]byte
	body     [][]byte
	traceID  [][]byte
	spanID   [][]byte
	attrs    [][]byte
}

func (c *recordCols) len() int { return len(c.ts) }

// newRecordCols returns an empty recordCols whose selected columns are pre-sized to hold n rows
// (so the accumulation copies never reallocate); unselected columns stay nil and are never touched.
func newRecordCols(n int, sel colSet) *recordCols {
	c := &recordCols{sel: sel, ts: make([]int64, 0, n)}
	if sel.observed {
		c.observed = make([]int64, 0, n)
	}

	if sel.severity {
		c.severity = make([]int64, 0, n)
	}

	if sel.flags {
		c.flags = make([]int64, 0, n)
	}

	if sel.dropped {
		c.dropped = make([]int64, 0, n)
	}

	if sel.sevText {
		c.sevText = make([][]byte, 0, n)
	}

	if sel.body {
		c.body = make([][]byte, 0, n)
	}

	if sel.traceID {
		c.traceID = make([][]byte, 0, n)
	}

	if sel.spanID {
		c.spanID = make([][]byte, 0, n)
	}

	if sel.attrs {
		c.attrs = make([][]byte, 0, n)
	}

	return c
}

// appendClone appends r, cloning its byte fields — for the head buffer, whose bytes outlive the
// caller's (which may reuse the ingest batch).
func (c *recordCols) appendClone(r rec) {
	c.ts = append(c.ts, r.ts)
	c.observed = append(c.observed, r.observed)
	c.severity = append(c.severity, r.severity)
	c.flags = append(c.flags, r.flags)
	c.dropped = append(c.dropped, r.dropped)
	c.sevText = append(c.sevText, bytes.Clone(r.sevText))
	c.body = append(c.body, bytes.Clone(r.body))
	c.traceID = append(c.traceID, bytes.Clone(r.traceID))
	c.spanID = append(c.spanID, bytes.Clone(r.spanID))
	c.attrs = append(c.attrs, bytes.Clone(r.attrs))
}

// appendRow appends row i of src as-is (no clone — src's bytes are already owned/stable). Only the
// destination's selected columns are copied; src must populate at least those (a part decodes the
// same set; the head buffer holds all). ts is always copied.
func (c *recordCols) appendRow(src *recordCols, i int) {
	s := &c.sel
	c.ts = append(c.ts, src.ts[i])

	if s.observed {
		c.observed = append(c.observed, src.observed[i])
	}

	if s.severity {
		c.severity = append(c.severity, src.severity[i])
	}

	if s.flags {
		c.flags = append(c.flags, src.flags[i])
	}

	if s.dropped {
		c.dropped = append(c.dropped, src.dropped[i])
	}

	if s.sevText {
		c.sevText = append(c.sevText, src.sevText[i])
	}

	if s.body {
		c.body = append(c.body, src.body[i])
	}

	if s.traceID {
		c.traceID = append(c.traceID, src.traceID[i])
	}

	if s.spanID {
		c.spanID = append(c.spanID, src.spanID[i])
	}

	if s.attrs {
		c.attrs = append(c.attrs, src.attrs[i])
	}
}

// appendRange bulk-appends rows [lo, hi) of src's selected columns — one append per column rather
// than per row. Byte slices are copied by reference (they alias src's decoded bytes).
func (c *recordCols) appendRange(src *recordCols, lo, hi int) {
	s := &c.sel
	c.ts = append(c.ts, src.ts[lo:hi]...)

	if s.observed {
		c.observed = append(c.observed, src.observed[lo:hi]...)
	}

	if s.severity {
		c.severity = append(c.severity, src.severity[lo:hi]...)
	}

	if s.flags {
		c.flags = append(c.flags, src.flags[lo:hi]...)
	}

	if s.dropped {
		c.dropped = append(c.dropped, src.dropped[lo:hi]...)
	}

	if s.sevText {
		c.sevText = append(c.sevText, src.sevText[lo:hi]...)
	}

	if s.body {
		c.body = append(c.body, src.body[lo:hi]...)
	}

	if s.traceID {
		c.traceID = append(c.traceID, src.traceID[lo:hi]...)
	}

	if s.spanID {
		c.spanID = append(c.spanID, src.spanID[lo:hi]...)
	}

	if s.attrs {
		c.attrs = append(c.attrs, src.attrs[lo:hi]...)
	}
}

// sortByTs reorders every column by ascending timestamp (stable, so equal-ts records keep their
// source order: older parts before the head). Records arrive part-ordered and a part's rows are
// already ts-sorted, so the accumulated window is very often already ordered — an O(n) check skips
// the O(n log n) sort and its permute allocations in that common case.
func (c *recordCols) sortByTs() {
	if c.isSortedByTs() {
		return
	}

	idx := make([]int, c.len())
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool { return c.ts[idx[a]] < c.ts[idx[b]] })

	s := &c.sel
	c.ts = permute(c.ts, idx)
	permuteIf(s.observed, &c.observed, idx)
	permuteIf(s.severity, &c.severity, idx)
	permuteIf(s.flags, &c.flags, idx)
	permuteIf(s.dropped, &c.dropped, idx)
	permuteIf(s.sevText, &c.sevText, idx)
	permuteIf(s.body, &c.body, idx)
	permuteIf(s.traceID, &c.traceID, idx)
	permuteIf(s.spanID, &c.spanID, idx)
	permuteIf(s.attrs, &c.attrs, idx)
}

// permuteIf reorders *col by idx only when active (a nil column stays nil).
func permuteIf[T any](active bool, col *[]T, idx []int) {
	if active {
		*col = permute(*col, idx)
	}
}

// isSortedByTs reports whether the timestamps are already non-decreasing.
func (c *recordCols) isSortedByTs() bool {
	for i := 1; i < len(c.ts); i++ {
		if c.ts[i] < c.ts[i-1] {
			return false
		}
	}

	return true
}

func permute[T any](s []T, idx []int) []T {
	out := make([]T, len(idx))
	for i, j := range idx {
		out[i] = s[j]
	}

	return out
}

// Column names of a flushed log part (and of a fetch batch's [fetch.NamedColumn]s). The stream id
// is the int128 identity/sort-grouping column; the rest are the per-record fields.
const (
	colStream   = "stream"
	colTs       = "ts"
	colObserved = "observed"
	colSeverity = "severity"
	colFlags    = "flags"
	colDropped  = "dropped"
	colSevText  = "severity_text"
	colBody     = "body"
	colTraceID  = "trace_id"
	colSpanID   = "span_id"
	colAttrs    = "attrs"
)

// isFixedColumn reports whether name is one of the fixed per-record columns (as opposed to a
// per-record attribute key, which lives in the serialized attrs column).
func isFixedColumn(name string) bool {
	switch name {
	case colTs, colObserved, colSeverity, colFlags, colDropped, colSevText, colBody, colTraceID, colSpanID, colAttrs:
		return true
	default:
		return false
	}
}

// toBatch builds the fetch batch for one stream from the accumulated columns, materializing only
// the projected columns (an empty projection materializes all). Timestamps always carries each
// record's event time.
func (c *recordCols) toBatch(id signal.SeriesID, s signal.Series, projection []string) *fetch.Batch {
	return &fetch.Batch{
		ID:         id,
		Series:     s,
		Timestamps: c.ts,
		Columns:    projectColumns(c, projection),
	}
}

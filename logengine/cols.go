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

// recordCols is a set of log records laid out columnarly (one entry per record across the parallel
// slices). It is the head's per-stream buffer, the per-stream slice of a flush, and a fetch
// accumulator. All slices share one length, [recordCols.len].
type recordCols struct {
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

// appendRow appends row i of src as-is (no clone — src's bytes are already owned/stable).
func (c *recordCols) appendRow(src *recordCols, i int) {
	c.ts = append(c.ts, src.ts[i])
	c.observed = append(c.observed, src.observed[i])
	c.severity = append(c.severity, src.severity[i])
	c.flags = append(c.flags, src.flags[i])
	c.dropped = append(c.dropped, src.dropped[i])
	c.sevText = append(c.sevText, src.sevText[i])
	c.body = append(c.body, src.body[i])
	c.traceID = append(c.traceID, src.traceID[i])
	c.spanID = append(c.spanID, src.spanID[i])
	c.attrs = append(c.attrs, src.attrs[i])
}

// sortByTs reorders every column by ascending timestamp (stable, so equal-ts records keep their
// source order: older parts before the head).
func (c *recordCols) sortByTs() {
	idx := make([]int, c.len())
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool { return c.ts[idx[a]] < c.ts[idx[b]] })

	c.ts = permute(c.ts, idx)
	c.observed = permute(c.observed, idx)
	c.severity = permute(c.severity, idx)
	c.flags = permute(c.flags, idx)
	c.dropped = permute(c.dropped, idx)
	c.sevText = permute(c.sevText, idx)
	c.body = permute(c.body, idx)
	c.traceID = permute(c.traceID, idx)
	c.spanID = permute(c.spanID, idx)
	c.attrs = permute(c.attrs, idx)
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

// toBatch builds the fetch batch for one stream from the accumulated columns. Timestamps carries
// each record's event time; the per-record fields are the named columns (the full set in M8a —
// projection narrows this later).
func (c *recordCols) toBatch(id signal.SeriesID, s signal.Series) *fetch.Batch {
	return &fetch.Batch{
		ID:         id,
		Series:     s,
		Timestamps: c.ts,
		Columns: []fetch.NamedColumn{
			{Name: colObserved, Int64: c.observed},
			{Name: colSeverity, Int64: c.severity},
			{Name: colFlags, Int64: c.flags},
			{Name: colDropped, Int64: c.dropped},
			{Name: colSevText, Bytes: c.sevText},
			{Name: colBody, Bytes: c.body},
			{Name: colTraceID, Bytes: c.traceID},
			{Name: colSpanID, Bytes: c.spanID},
			{Name: colAttrs, Bytes: c.attrs},
		},
	}
}

package recordengine

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// ErrNoSuchColumn is returned when a [ValuesRequest] names a column the schema does not have or one
// that is not byte-typed. Numeric columns hold no dictionary to enumerate — a signal's numeric enums
// (a span kind, a status code) are a static set the caller already knows.
var ErrNoSuchColumn = errors.New("recordengine: no such byte column")

// ValuesRequest selects a distinct-value enumeration. Exactly one of Column (a byte column of the
// schema) and AttrKey (one key inside the serialized per-record attributes column) must be set.
// Start/End bound the window, a zero start AND end disabling the time filter; Limit ≤ 0 is
// unlimited.
type ValuesRequest struct {
	Column     string
	AttrKey    []byte
	Start, End int64
	Limit      int
}

// ColumnValues enumerates the distinct values a byte column — or one per-record attribute key —
// takes within [start, end], without materializing rows. A flushed part answers from its column
// dictionary (O(distinct), no row decode); only the unflushed head is scanned, bounded by its size.
//
// The result is sorted, deduplicated, and owned by the caller. It is a **superset** for the window:
// a part whose bounds overlap the window contributes its whole dictionary, so a value that occurs
// only outside the window may be returned. That matches the fetch contract and is what tag/label
// autocomplete wants. Empty values are omitted. Limit truncates the sorted union, so the result is
// the lexicographically smallest Limit values — truncation is not otherwise signaled.
//
// Attribute values are the canonical text projection ([signal.Value.AppendText]), the same form the
// matching layer compares against. Safe for concurrent use.
func (e *Engine) ColumnValues(ctx context.Context, req ValuesRequest) ([][]byte, error) {
	k, err := e.valuesColumn(req)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	acc := valueAccumulator{seen: seen, attrKey: req.AttrKey}

	parts := e.collectHeadValues(&acc, k, req)
	defer func() {
		for _, p := range parts {
			p.release()
		}
	}()

	name := e.cfg.Schema.byteColumn(k).Name

	for _, p := range parts {
		dc, err := p.readDict(ctx, name, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "read column %q of part %q", name, p.prefix)
		}

		// Entries is the part's distinct value set in the dictionary form and one value per row in
		// the flat fallback (IDWidth 0); the accumulator dedups, so both forms read the same way.
		for _, entry := range dc.Entries {
			acc.add(entry)
		}
	}

	return sortedValues(seen, req.Limit), nil
}

// valuesColumn resolves the request to an index into the schema's byte-column vector: the named
// column, or the serialized-attributes column when the request enumerates an attribute key.
func (e *Engine) valuesColumn(req ValuesRequest) (int, error) {
	schema := e.cfg.Schema

	if len(req.AttrKey) > 0 {
		if req.Column != "" {
			return 0, errors.New("recordengine: column values: set either Column or AttrKey, not both")
		}

		k, ok := schema.attrsByteCol()
		if !ok {
			return 0, errors.Wrap(ErrNoSuchColumn, "signal has no per-record attributes column")
		}

		return k, nil
	}

	ref, ok := schema.ref(req.Column)
	if !ok || ref.kind != KindBytes {
		return 0, errors.Wrapf(ErrNoSuchColumn, "column %q", req.Column)
	}

	return ref.idx, nil
}

// collectHeadValues adds the values buffered in the head (plus any records detached mid-flush) to
// acc and returns the in-window parts, acquired so they stay readable once the lock is dropped —
// the backend reads that follow must not hold it.
func (e *Engine) collectHeadValues(acc *valueAccumulator, k int, req ValuesRequest) []*part {
	e.mu.RLock()
	defer e.mu.RUnlock()

	acc.addBuffers(e.head.records, k, req.Start, req.End)
	acc.addBuffers(e.flushing, k, req.Start, req.End)

	var live []*part

	for _, p := range e.readablePartsLocked() {
		if !partInWindow(p, req.Start, req.End) {
			continue
		}

		p.acquire()
		live = append(live, p)
	}

	return live
}

// valueAccumulator collects distinct values as owned map keys. With attrKey set it treats each cell
// as a serialized attribute blob and keeps only that key's value, so a part's dictionary is decoded
// once per *distinct blob* rather than once per row.
type valueAccumulator struct {
	seen    map[string]struct{}
	attrKey []byte
	attrs   signal.Attributes // reused decode buffer
	text    []byte            // reused text-projection buffer
}

// add records one cell's contribution: the cell itself, or the values its attribute blob holds for
// the requested key. A malformed blob is skipped — enumeration is best-effort metadata.
func (a *valueAccumulator) add(cell []byte) {
	if a.attrKey == nil {
		if len(cell) > 0 {
			a.seen[string(cell)] = struct{}{}
		}

		return
	}

	if len(cell) == 0 {
		return
	}

	attrs, _, err := signal.AppendAttributes(a.attrs[:0], cell)
	if err != nil {
		return
	}

	a.attrs = attrs

	for i := range attrs {
		if !bytes.Equal(attrs[i].Key, a.attrKey) {
			continue
		}

		a.text = attrs[i].Value.AppendText(a.text[:0])
		if len(a.text) > 0 {
			a.seen[string(a.text)] = struct{}{}
		}
	}
}

// addBuffers adds column k's in-window cells from every buffered stream. A zero start AND end
// disables the time filter. Caller holds the engine lock.
func (a *valueAccumulator) addBuffers(records map[signal.SeriesID]*recordCols, k int, start, end int64) {
	all := start == 0 && end == 0

	for _, buf := range records {
		if buf.len() == 0 {
			continue
		}

		cells := buf.cellsAt(k)
		whole := all || (start <= buf.tsMin && buf.tsMax <= end)

		for i := range buf.ts {
			if !whole && (buf.ts[i] < start || buf.ts[i] > end) {
				continue
			}

			a.add(cells.at(i))
		}
	}
}

// sortedValues projects the accumulated set into a sorted slice, truncated to limit (≤ 0 ⇒ all).
func sortedValues(seen map[string]struct{}, limit int) [][]byte {
	if len(seen) == 0 {
		return nil
	}

	out := make([][]byte, 0, len(seen))
	for v := range seen {
		out = append(out, []byte(v))
	}

	slices.SortFunc(out, bytes.Compare)

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out
}

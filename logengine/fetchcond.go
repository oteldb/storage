package logengine

import (
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// colValue returns the typed value of the named column for row i, and whether the engine has that
// column. A name that is not one of the fixed record columns is looked up in the row's serialized
// attributes (so a Condition can target a per-record attribute key); an absent attribute yields
// (empty, false) — a positive predicate then does not match, and the language layer composes any
// negation/absent semantics.
func (c *recordCols) colValue(i int, name string) (signal.Value, bool) {
	switch name {
	case colTs:
		return signal.IntValue(c.ts[i]), true
	case colObserved:
		return signal.IntValue(c.observed[i]), true
	case colSeverity:
		return signal.IntValue(c.severity[i]), true
	case colFlags:
		return signal.IntValue(c.flags[i]), true
	case colDropped:
		return signal.IntValue(c.dropped[i]), true
	case colSevText:
		return signal.StringValue(c.sevText[i]), true
	case colBody:
		return signal.StringValue(c.body[i]), true
	case colTraceID:
		return signal.BytesValue(c.traceID[i]), true
	case colSpanID:
		return signal.BytesValue(c.spanID[i]), true
	default:
		// A non-fixed name is a per-record attribute: scan the serialized blob for just that
		// key (no full-map decode, no allocation).
		v, ok, err := signal.LookupAttribute(c.attrs[i], name)
		if err != nil || !ok {
			return signal.Value{}, false
		}

		return v, true
	}
}

// rowMatches reports whether row i satisfies every condition (logical AND). A condition over an
// absent attribute does not match. Conditions are applied only when the caller has decided to (it
// passes AllConditions).
func (c *recordCols) rowMatches(i int, conds []fetch.Condition) bool {
	for j := range conds {
		v, ok := c.colValue(i, conds[j].Column)
		if !ok || !conds[j].Match(v) {
			return false
		}
	}

	return true
}

// filterInPlace compacts the columns to keep only the rows satisfying all conditions (AND),
// reusing the backing arrays — no new allocation (a select-all filter is just a no-op truncate).
func (c *recordCols) filterInPlace(conds []fetch.Condition) {
	w := 0
	for i := range c.ts {
		if !c.rowMatches(i, conds) {
			continue
		}

		if w != i {
			c.moveRow(i, w)
		}

		w++
	}

	c.truncate(w)
}

// moveRow overwrites row to with row from (a backward compaction step), for the selected columns.
func (c *recordCols) moveRow(from, to int) {
	s := &c.sel
	c.ts[to] = c.ts[from]

	if s.observed {
		c.observed[to] = c.observed[from]
	}

	if s.severity {
		c.severity[to] = c.severity[from]
	}

	if s.flags {
		c.flags[to] = c.flags[from]
	}

	if s.dropped {
		c.dropped[to] = c.dropped[from]
	}

	if s.sevText {
		c.sevText[to] = c.sevText[from]
	}

	if s.body {
		c.body[to] = c.body[from]
	}

	if s.traceID {
		c.traceID[to] = c.traceID[from]
	}

	if s.spanID {
		c.spanID[to] = c.spanID[from]
	}

	if s.attrs {
		c.attrs[to] = c.attrs[from]
	}
}

// truncate shortens every selected column to n rows.
func (c *recordCols) truncate(n int) {
	s := &c.sel
	c.ts = c.ts[:n]
	truncateIf(s.observed, &c.observed, n)
	truncateIf(s.severity, &c.severity, n)
	truncateIf(s.flags, &c.flags, n)
	truncateIf(s.dropped, &c.dropped, n)
	truncateIf(s.sevText, &c.sevText, n)
	truncateIf(s.body, &c.body, n)
	truncateIf(s.traceID, &c.traceID, n)
	truncateIf(s.spanID, &c.spanID, n)
	truncateIf(s.attrs, &c.attrs, n)
}

func truncateIf[T any](active bool, col *[]T, n int) {
	if active {
		*col = (*col)[:n]
	}
}

// projectColumns is the set of named columns to materialize in a batch, given a projection. An
// empty projection materializes every column. Unknown projection names are ignored.
func projectColumns(c *recordCols, projection []string) []fetch.NamedColumn {
	if len(projection) == 0 {
		return c.allColumns()
	}

	out := make([]fetch.NamedColumn, 0, len(projection))
	for _, name := range projection {
		if col, ok := c.namedColumn(name); ok {
			out = append(out, col)
		}
	}

	return out
}

// namedColumn returns the named per-record column (ts is carried by Batch.Timestamps, not here).
func (c *recordCols) namedColumn(name string) (fetch.NamedColumn, bool) {
	switch name {
	case colObserved:
		return fetch.NamedColumn{Name: colObserved, Int64: c.observed}, true
	case colSeverity:
		return fetch.NamedColumn{Name: colSeverity, Int64: c.severity}, true
	case colFlags:
		return fetch.NamedColumn{Name: colFlags, Int64: c.flags}, true
	case colDropped:
		return fetch.NamedColumn{Name: colDropped, Int64: c.dropped}, true
	case colSevText:
		return fetch.NamedColumn{Name: colSevText, Bytes: c.sevText}, true
	case colBody:
		return fetch.NamedColumn{Name: colBody, Bytes: c.body}, true
	case colTraceID:
		return fetch.NamedColumn{Name: colTraceID, Bytes: c.traceID}, true
	case colSpanID:
		return fetch.NamedColumn{Name: colSpanID, Bytes: c.spanID}, true
	case colAttrs:
		return fetch.NamedColumn{Name: colAttrs, Bytes: c.attrs}, true
	default:
		return fetch.NamedColumn{}, false
	}
}

// allColumns returns every per-record column (the default, full projection).
func (c *recordCols) allColumns() []fetch.NamedColumn {
	return []fetch.NamedColumn{
		{Name: colObserved, Int64: c.observed},
		{Name: colSeverity, Int64: c.severity},
		{Name: colFlags, Int64: c.flags},
		{Name: colDropped, Int64: c.dropped},
		{Name: colSevText, Bytes: c.sevText},
		{Name: colBody, Bytes: c.body},
		{Name: colTraceID, Bytes: c.traceID},
		{Name: colSpanID, Bytes: c.spanID},
		{Name: colAttrs, Bytes: c.attrs},
	}
}

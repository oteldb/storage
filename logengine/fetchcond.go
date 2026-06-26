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
		attrs, _, err := signal.DecodeAttributes(c.attrs[i])
		if err != nil {
			return signal.Value{}, false
		}

		v, ok := attrs.Get([]byte(name))

		return v, ok
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

// filtered returns a new recordCols holding only the rows satisfying all conditions (AND).
func (c *recordCols) filtered(conds []fetch.Condition) *recordCols {
	out := &recordCols{}
	for i := range c.ts {
		if c.rowMatches(i, conds) {
			out.appendRow(c, i)
		}
	}

	return out
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

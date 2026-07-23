package recordengine

import (
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// colValue returns the typed value of the named column for row i. The implicit timestamp and any
// fixed schema column read their vector directly (int → IntValue, bytes → StringValue: the raw
// bytes, which v.Str interprets and a language predicate matches as it sees fit). A name that is
// not a fixed column is looked up in the serialized attributes blobs (the schema's [BloomAttrs]
// columns, in declaration order — the first hit wins) via the zero-allocation
// [signal.LookupAttribute].
//
// [lazyCols.colValue] is the deliberate byte-for-byte twin of this over a lazy dictionary-column
// source; the two are kept separate (not factored through a shared accessor) because this runs in
// the per-row scan hot path, where accessor closures measurably regress it.
func (c *recordCols) colValue(i int, name string) (signal.Value, bool) {
	if name == colTs {
		return signal.IntValue(c.ts[i]), true
	}

	if ref, ok := c.schema.ref(name); ok {
		if ref.kind == KindInt64 {
			return signal.IntValue(c.ints[ref.idx][i]), true
		}

		return signal.StringValue(c.bytes[ref.idx].at(i)), true
	}

	for _, k := range c.schema.attrsByteCols() {
		v, found, err := signal.LookupAttribute(c.bytes[k].at(i), name)
		if err == nil && found {
			return v, true
		}
	}

	return signal.Value{}, false
}

// rowMatches reports whether row i satisfies every condition (logical AND). A column the row does
// not carry is offered to the predicate as [signal.EmptyValue], never treated as a non-match: the
// condition is operator-free, so only the language knows whether an absent value satisfies it (a
// negation or an is-unset predicate does).
func (c *recordCols) rowMatches(i int, conds []fetch.Condition) bool {
	for j := range conds {
		v, ok := c.colValue(i, conds[j].Column)
		if !ok {
			v = signal.EmptyValue()
		}

		if !conds[j].Match(v) {
			return false
		}
	}

	return true
}

// filterInPlace compacts the columns to keep only the rows satisfying all conditions (AND), reusing
// the backing arrays — no new allocation (a select-all filter is a no-op). It collects the surviving
// row indices into the reusable rowScratch, then gathers every selected column to them.
func (c *recordCols) filterInPlace(conds []fetch.Condition) {
	idx := c.rowScratch[:0]
	for i := range c.ts {
		if c.rowMatches(i, conds) {
			idx = append(idx, i)
		}
	}

	c.rowScratch = idx
	if len(idx) == len(c.ts) {
		return // every row survived — nothing to compact
	}

	c.gatherRows(idx)
}

// gatherRows compacts every selected column (ts always) to the rows named by idx (strictly
// increasing), in place. The int/timestamp columns move by index; each byte column rewrites its blob
// forward via [byteCol.gather].
func (c *recordCols) gatherRows(idx []int) {
	for p, i := range idx {
		c.ts[p] = c.ts[i]
	}

	c.ts = c.ts[:len(idx)]

	for k := range c.ints {
		if c.sel.ints[k] {
			col := c.ints[k]
			for p, i := range idx {
				col[p] = col[i]
			}

			c.ints[k] = col[:len(idx)]
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k].gather(idx)
		}
	}
}

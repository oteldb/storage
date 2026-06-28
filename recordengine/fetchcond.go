package recordengine

import (
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// colValue returns the typed value of the named column for row i. The implicit timestamp and any
// fixed schema column read their vector directly (int → IntValue, bytes → StringValue: the raw
// bytes, which v.Str interprets and a language predicate matches as it sees fit). A name that is
// not a fixed column is looked up in the serialized attributes blob (the schema's [BloomAttrs]
// column) — a per-record attribute key, found via the zero-allocation [signal.LookupAttribute].
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

	k, ok := c.schema.attrsByteCol()
	if !ok {
		return signal.Value{}, false
	}

	v, found, err := signal.LookupAttribute(c.bytes[k].at(i), name)
	if err != nil || !found {
		return signal.Value{}, false
	}

	return v, true
}

// rowMatches reports whether row i satisfies every condition (logical AND).
func (c *recordCols) rowMatches(i int, conds []fetch.Condition) bool {
	for j := range conds {
		v, ok := c.colValue(i, conds[j].Column)
		if !ok || !conds[j].Match(v) {
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

package recordengine

import (
	"context"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// The filtered fetch path (AllConditions with conditions) reads a part in two phases so a
// high-selectivity lookup (e.g. trace-by-id) never materializes the projected byte columns for rows
// it will discard. Phase 1 decodes only the timestamp, the int columns, and the *condition* byte
// columns (as lazy [chunk.DictColumn]s, whose per-row At is O(1) and copies nothing) and scans for
// matching rows; a part with no match (a bloom false positive) is dropped without touching its
// projected byte columns at all. Phase 2 decodes the remaining projected byte columns and gathers
// only the matched rows' cells into the per-stream accumulators.
//
// This complements the unfiltered path ([fetchPlan.readParts]), which bulk-decodes every selected
// column because a pure matcher fetch returns whole per-stream ranges.

// lazyCols holds a part's decoded timestamp and int columns (materialized — cheap, contiguous) plus
// its selected byte columns as lazy dictionary columns, so a conditions scan reads byte cells by
// index without copying the whole column.
type lazyCols struct {
	schema *Schema
	ts     []int64
	ints   [][]int64
	bytes  []*chunk.DictColumn // one per schema byte column; nil until decoded
}

// conditionSel is the subset of byte/int columns a request's conditions read (the columns phase 1
// must decode to evaluate matches). A condition over a per-record attribute key resolves to the
// schema's attrs byte column.
func conditionSel(s *Schema, conds []fetch.Condition) colSel {
	sel := colSel{ints: make([]bool, s.numInts()), bytes: make([]bool, s.numBytes())}

	for i := range conds {
		name := conds[i].Column
		if name == colTs {
			continue
		}

		if ref, ok := s.ref(name); ok {
			if ref.kind == KindInt64 {
				sel.ints[ref.idx] = true
			} else {
				sel.bytes[ref.idx] = true
			}

			continue
		}

		if k, ok := s.attrsByteCol(); ok {
			sel.bytes[k] = true
		}
	}

	return sel
}

// readLazyConds decodes phase 1: the timestamp, every selected int column, and the condition byte
// columns (condSel). getI64 supplies pooled int scratch (recycled by [Engine.recycleLazyInts]).
func (p *part) readLazyConds(ctx context.Context, sel, condSel colSel, getI64 func() []int64) (*lazyCols, error) {
	lz := &lazyCols{schema: p.schema, ints: make([][]int64, p.schema.numInts()), bytes: make([]*chunk.DictColumn, p.schema.numBytes())}

	dst := func() []int64 {
		if getI64 != nil {
			return getI64()
		}

		return nil
	}

	var err error
	if lz.ts, err = p.readInt64(ctx, colTs, dst()); err != nil {
		return nil, err
	}

	for k := range lz.ints {
		if sel.ints[k] {
			if lz.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, dst()); err != nil {
				return nil, err
			}
		}
	}

	for k := range lz.bytes {
		if condSel.bytes[k] {
			if lz.bytes[k], err = p.readDict(ctx, p.schema.byteColumn(k).Name); err != nil {
				return nil, err
			}
		}
	}

	return lz, nil
}

// decodeProjected decodes phase 2: the selected byte columns not already decoded in phase 1. Called
// only when the part holds at least one matching row.
func (p *part) decodeProjected(ctx context.Context, lz *lazyCols, sel, condSel colSel) error {
	for k := range lz.bytes {
		if sel.bytes[k] && !condSel.bytes[k] && lz.bytes[k] == nil {
			dc, err := p.readDict(ctx, p.schema.byteColumn(k).Name)
			if err != nil {
				return err
			}

			lz.bytes[k] = dc
		}
	}

	return nil
}

func (p *part) readDict(ctx context.Context, name string) (*chunk.DictColumn, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	return col.Bytes()
}

// colValue is the byte-for-byte twin of [recordCols.colValue] over the lazy dictionary-column source
// (byte cells via [chunk.DictColumn.At]). Kept separate rather than factored through a shared
// accessor because it runs in the per-row scan hot path, where accessor closures measurably regress
// it (~30%).
//
//nolint:dupl // intentional hot-path twin of recordCols.colValue; see the doc above.
func (lz *lazyCols) colValue(i int, name string) (signal.Value, bool) {
	if name == colTs {
		return signal.IntValue(lz.ts[i]), true
	}

	if ref, ok := lz.schema.ref(name); ok {
		if ref.kind == KindInt64 {
			return signal.IntValue(lz.ints[ref.idx][i]), true
		}

		return signal.StringValue(lz.bytes[ref.idx].At(i)), true
	}

	k, ok := lz.schema.attrsByteCol()
	if !ok {
		return signal.Value{}, false
	}

	v, found, err := signal.LookupAttribute(lz.bytes[k].At(i), name)
	if err != nil || !found {
		return signal.Value{}, false
	}

	return v, true
}

// rowMatches reports whether row i satisfies every condition (logical AND).
func (lz *lazyCols) rowMatches(i int, conds []fetch.Condition) bool {
	for j := range conds {
		v, ok := lz.colValue(i, conds[j].Column)
		if !ok || !conds[j].Match(v) {
			return false
		}
	}

	return true
}

// appendLazyRow appends row i of lz's selected columns (ts always) to c, copying byte cells (via
// [chunk.DictColumn.At]) into c's blob so they no longer alias the part.
func (c *recordCols) appendLazyRow(lz *lazyCols, i int) {
	c.ts = append(c.ts, lz.ts[i])
	c.noteTS(lz.ts[i])

	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = append(c.ints[k], lz.ints[k][i])
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k].appendCell(lz.bytes[k].At(i))
		}
	}
}

// readPartsLazy is the two-phase filtered part scan (see file doc). It appends only matching,
// in-window rows to the per-stream accumulators; the caller's post-scan filterInPlace re-applies the
// conditions to the head-seeded rows (part rows already pass, so it only compacts).
func (p *fetchPlan) readPartsLazy(ctx context.Context) error {
	for _, part := range p.liveParts {
		lz, err := part.readLazyConds(ctx, p.sel, p.condSel, p.e.getI64)
		if err != nil {
			return err
		}

		// Phase 1: a non-matching part (a bloom false positive) never decodes its projected columns.
		if p.partHasMatch(part, lz) {
			// Phase 2: decode the projected byte columns and gather only the matched rows.
			if err := part.decodeProjected(ctx, lz, p.sel, p.condSel); err != nil {
				return err
			}

			p.gatherMatches(part, lz)
		}

		p.e.recycleLazyInts(lz)
	}

	return nil
}

// rowMatch reports whether row i is in the query window and satisfies all conditions.
func (p *fetchPlan) rowMatch(lz *lazyCols, i int) bool {
	return lz.ts[i] >= p.start && lz.ts[i] <= p.end && lz.rowMatches(i, p.conds)
}

// partHasMatch reports whether any requested stream holds an in-window matching row, short-circuiting
// on the first hit so a matching part decodes its projected columns exactly once.
func (p *fetchPlan) partHasMatch(part *part, lz *lazyCols) bool {
	for _, id := range p.ids {
		rng, ok := part.ranges[id]
		if !ok {
			continue
		}

		for i := rng.start; i < rng.end; i++ {
			if p.rowMatch(lz, i) {
				return true
			}
		}
	}

	return false
}

// gatherMatches appends every requested stream's matching, in-window rows to its accumulator.
func (p *fetchPlan) gatherMatches(part *part, lz *lazyCols) {
	for _, id := range p.ids {
		rng, ok := part.ranges[id]
		if !ok {
			continue
		}

		acc := p.accs[id]
		for i := rng.start; i < rng.end; i++ {
			if p.rowMatch(lz, i) {
				acc.appendLazyRow(lz, i)
			}
		}
	}
}

// recycleLazyInts returns a lazy part's decoded int columns (timestamp + int values) to the pool;
// their values are already copied into the accumulators. The dictionary columns are left to the GC.
func (e *Engine) recycleLazyInts(lz *lazyCols) {
	e.putI64(lz.ts)
	for k := range lz.ints {
		e.putI64(lz.ints[k])
	}
}

package recordengine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/simd"
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
	bytes  []*chunk.DictColumn // one per schema byte column; nil until decoded (dict/flat-fallback path)

	// rawBlob holds, per schema byte column, the flat [chunk.CodecBytesRaw] blob [eqFastPathCols]
	// decoded instead of a [chunk.DictColumn] — row i's value is rawBlob[k].blob[i*width:(i+1)*width].
	// Zero (nil blob) means the column went through the normal bytes[k] dict-decode path instead.
	// Populated whenever the fast path fires, whether or not the column is also projected: unlike
	// bytes[k], a blob slice needs no extra decode to serve a projected column's per-row value too.
	rawBlob []rawBytesCol

	// eqMask holds, per condition (parallel to the request's conds), a precomputed whole-column
	// equality bitmap (rowMatches[i] == 1 if and only if row i matches) from a [simd.EqualFixed16]
	// scan — the fast path [eqFastPathCols] selects when a condition is an exact match against a
	// [chunk.CodecBytesRaw] column with no other condition targeting it. nil, or a nil entry, means
	// that condition falls back to the normal colValue+Match per-row check.
	eqMask [][]byte
}

// rawBytesCol is one column's flat fixed-width blob (see [lazyCols.rawBlob]): row i is
// blob[i*width:(i+1)*width]. The zero value (nil blob) means "not decoded this way".
type rawBytesCol struct {
	blob  []byte
	width int
}

// at returns row i's value from the blob, or (nil, false) if this column wasn't raw-decoded.
func (rb rawBytesCol) at(i int) ([]byte, bool) {
	if rb.blob == nil {
		return nil, false
	}

	return rb.blob[i*rb.width : (i+1)*rb.width], true
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

// eqFastPathCols returns, for each byte column index a fast whole-column equality scan may
// replace its dictionary decode for, the index of the one condition (in conds) it serves — as
// long as that's safe for every consumer of the column:
//   - the condition is an exact match ([fetch.Condition.Equal]) with a [simd.EqualFixed16Width]-byte
//     value — the fixed width the kernel scans;
//   - the column's on-disk codec is [chunk.CodecBytesRaw] (a contiguous fixed-width blob, not a
//     dictionary — [block.ColumnReader.BytesRaw] errors otherwise, so this is a static
//     pre-check, not a guarantee); and
//   - no other condition targets the same column (skipping its dict decode must not break a
//     second condition that needs per-row values via colValue).
//
// A fast-pathed column may still be projected: [lazyCols.rawBlob] serves a projected row's value
// straight from the decoded blob (no [chunk.DictColumn] needed), so — unlike an earlier version of
// this check — being in the projection does not disqualify a column. This matters in practice: the
// by-id lookups ([storage.Storage.Trace]/[storage.Storage.LogsForTrace]) set no Projection at all
// (every column, trace_id included, is selected), which is exactly the common case this optimizes.
//
// Precondition this relies on: a fast-pathed condition's [fetch.Condition.Match] is never called —
// [lazyCols.rowMatches] takes the precomputed [lazyCols.eqMask] bit instead. [fetch.Condition]'s own
// contract says Equal is only ever a prune *hint* ("the engine always re-checks Match per row, so a
// hint only ever skips work, never changes results"); this function's caller must only set Equal
// when it is byte-identical to Match for every row of this column (true of every current caller —
// see fetchByEquality) — Equal must never be a lossy/approximate stand-in (case-folded, normalized,
// or otherwise broader than Match) or the fast path will silently return wrong rows.
func eqFastPathCols(schema *Schema, conds []fetch.Condition) map[int]int {
	refCount := make(map[int]int, len(conds))
	for j := range conds {
		if ref, ok := schema.ref(conds[j].Column); ok && ref.kind == KindBytes {
			refCount[ref.idx]++
		}
	}

	fast := make(map[int]int)
	for j := range conds {
		cond := &conds[j]
		if cond.Equal == nil || len(cond.Equal.Value) != simd.EqualFixed16Width {
			continue
		}

		ref, ok := schema.ref(cond.Column)
		if !ok || ref.kind != KindBytes {
			continue
		}

		if refCount[ref.idx] != 1 {
			continue
		}

		if schema.byteColumn(ref.idx).Codec != chunk.CodecBytesRaw {
			continue
		}

		fast[ref.idx] = j
	}

	return fast
}

// rawBytesBlob decodes column name's flat [chunk.CodecBytesRaw] blob. ok is false (blob, err both
// zero/nil) if the column isn't actually raw-decodable this way (e.g. it is block-framed, which
// [block.ColumnReader.BytesRaw] does not support yet) — the caller falls back to the normal
// per-row dict-decode+Match path in that case, so this is an optimization, never a correctness
// requirement.
func (p *part) rawBytesBlob(ctx context.Context, name string) (blob []byte, width int, ok bool, err error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, 0, false, err
	}

	blob, width, err = col.BytesRaw()
	if err != nil {
		return nil, 0, false, nil //nolint:nilerr // not fixed-width: fall back to dict decode
	}

	return blob, width, true, nil
}

// readLazyConds decodes phase 1: the timestamp, every selected int column, and the condition byte
// columns (condSel). getI64 supplies pooled int scratch (recycled by [Engine.recycleLazyInts]).
func (p *part) readLazyConds(ctx context.Context, sel, condSel colSel, conds []fetch.Condition, getI64 func() []int64) (*lazyCols, error) {
	lz := &lazyCols{
		schema:  p.schema,
		ints:    make([][]int64, p.schema.numInts()),
		bytes:   make([]*chunk.DictColumn, p.schema.numBytes()),
		rawBlob: make([]rawBytesCol, p.schema.numBytes()),
	}
	if len(conds) > 0 {
		lz.eqMask = make([][]byte, len(conds))
	}

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

	fast := eqFastPathCols(p.schema, conds)

	for k := range lz.bytes {
		if !condSel.bytes[k] {
			continue
		}

		if j, isFast := fast[k]; isFast {
			needle := []byte(conds[j].Equal.Value)

			blob, width, ok, err := p.rawBytesBlob(ctx, p.schema.byteColumn(k).Name)
			if err != nil {
				return nil, err
			}

			// width must be exactly simd.EqualFixed16Width — the kernel's real, hardcoded
			// precondition — not merely equal to needle's length (eqFastPathCols already fixes
			// that at 16 separately; comparing against len(needle) here would assert the wrong
			// thing if that ever changed).
			if ok && width == simd.EqualFixed16Width {
				mask := make([]byte, len(blob)/width)
				simd.EqualFixed16(blob, needle, mask)

				lz.eqMask[j] = mask
				lz.rawBlob[k] = rawBytesCol{blob: blob, width: width}

				continue
			}
		}

		if lz.bytes[k], err = p.readDict(ctx, p.schema.byteColumn(k).Name); err != nil {
			return nil, err
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

// colValue is the twin of [recordCols.colValue] over the lazy dictionary-column source (byte cells
// via [chunk.DictColumn.At], or a fast-pathed column's flat blob via [lazyCols.rawBlob]). Kept
// separate rather than factored through a shared accessor because it runs in the per-row scan hot
// path, where accessor closures measurably regress it (~30%).
func (lz *lazyCols) colValue(i int, name string) (signal.Value, bool) {
	if name == colTs {
		return signal.IntValue(lz.ts[i]), true
	}

	if ref, ok := lz.schema.ref(name); ok {
		if ref.kind == KindInt64 {
			return signal.IntValue(lz.ints[ref.idx][i]), true
		}

		if v, ok := lz.rawBlob[ref.idx].at(i); ok {
			return signal.StringValue(v), true
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

// rowMatches reports whether row i satisfies every condition (logical AND). A condition with a
// precomputed [lazyCols.eqMask] entry (see [eqFastPathCols]) reads its row's bit instead of going
// through colValue+Match. Absent columns follow [recordCols.rowMatches]: the predicate is offered
// [signal.EmptyValue] rather than short-circuited to a non-match.
func (lz *lazyCols) rowMatches(i int, conds []fetch.Condition) bool {
	for j := range conds {
		if len(lz.eqMask) != 0 && lz.eqMask[j] != nil {
			if lz.eqMask[j][i] == 0 {
				return false
			}

			continue
		}

		v, ok := lz.colValue(i, conds[j].Column)
		if !ok {
			v = signal.EmptyValue()
		}

		if !conds[j].Match(v) {
			return false
		}
	}

	return true
}

// appendLazyRow appends row i of lz's selected columns (ts always) to c, copying byte cells (via
// [chunk.DictColumn.At], or the flat blob directly for an [eqFastPathCols] column) into c's blob
// so they no longer alias the part.
func (c *recordCols) appendLazyRow(lz *lazyCols, i int) {
	c.ts = append(c.ts, lz.ts[i])
	c.noteTS(lz.ts[i])

	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = append(c.ints[k], lz.ints[k][i])
		}
	}

	for k := range c.bytes {
		if !c.sel.bytes[k] {
			continue
		}

		if v, ok := lz.rawBlob[k].at(i); ok {
			c.bytes[k].appendCell(v)
			continue
		}

		c.bytes[k].appendCell(lz.bytes[k].At(i))
	}
}

// readPartsLazy is the two-phase filtered part scan (see file doc). It appends only matching,
// in-window rows to the per-stream accumulators; the caller's post-scan filterInPlace re-applies the
// conditions to the head-seeded rows (part rows already pass, so it only compacts).
func (p *fetchPlan) readPartsLazy(ctx context.Context) error {
	for _, part := range p.liveParts {
		lz, err := part.readLazyConds(ctx, p.sel, p.condSel, p.conds, p.e.getI64)
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

// tsWindow narrows a stream's part range [rng.start, rng.end) to the sub-range whose timestamps fall
// in [start, end]. Part rows are (stream, ts)-sorted, so each stream's range is ts-ascending and the
// two bounds are found by binary search — the scan then evaluates conditions on in-window rows only,
// with no per-row timestamp compare.
func tsWindow(ts []int64, rng rowRange, start, end int64) rowRange {
	sub := ts[rng.start:rng.end]

	// lo: first ts >= start. hi: first ts > end — expressed via a comparator (not BinarySearch of
	// end+1, which overflows at end == math.MaxInt64, the open-ended query upper bound).
	lo, _ := slices.BinarySearch(sub, start)
	hi, _ := slices.BinarySearchFunc(sub, end, func(e, target int64) int {
		if e > target {
			return +1
		}

		return -1
	})

	return rowRange{start: rng.start + lo, end: rng.start + hi}
}

// partHasMatch reports whether any requested stream holds an in-window matching row, short-circuiting
// on the first hit so a matching part decodes its projected columns exactly once.
func (p *fetchPlan) partHasMatch(part *part, lz *lazyCols) bool {
	for _, id := range p.ids {
		rng, ok := part.ranges[id]
		if !ok {
			continue
		}

		w := tsWindow(lz.ts, rng, p.start, p.end)
		for i := w.start; i < w.end; i++ {
			if lz.rowMatches(i, p.conds) {
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

		w := tsWindow(lz.ts, rng, p.start, p.end)
		acc := p.accs[id]
		for i := w.start; i < w.end; i++ {
			if lz.rowMatches(i, p.conds) {
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

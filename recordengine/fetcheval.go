package recordengine

import (
	"cmp"
	"slices"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// The filtered scan evaluates its conditions through a plan compiled once per part rather than
// re-resolving each condition's column and re-running its predicate for every row. Three things fall
// out of compiling once:
//
//   - Column resolution (a schema map lookup) happens once per condition per part, not once per
//     condition per row.
//   - A dictionary-encoded column's predicate is memoized per *distinct entry*. The byte columns
//     carry at most 65536 entries, so a regex or attribute-lookup predicate over a million rows
//     collapses to at most one call per entry — and, because the memo fills lazily, a
//     high-selectivity scan that touches five rows still pays only five calls. Attributes benefit
//     most: without the memo, [signal.LookupAttribute] re-parses the attrs blob per condition per
//     row.
//   - An int column's predicate is memoized per distinct value over a small fixed domain, which is
//     where enum-shaped columns (severity, status codes) live.
//   - Conditions run cheap-first (the PREWHERE analog): a precomputed equality bitmap rejects a
//     row before a memoized dictionary or value probe, which rejects it before a per-row call.
//
// Reordering is sound because the conditions are a logical AND of pure predicates; a condition's
// [fetch.Condition.Match] is a callback, so this changes only how often it is called, never with
// what.

// evalKind is how a compiled condition tests a row. The values are ordered cheapest-first —
// [fetchPlan.compileConds] sorts by them.
type evalKind uint8

const (
	// evalEqMask reads a precomputed whole-column equality bitmap ([lazyCols.eqMask]): one byte load,
	// no predicate call at all.
	evalEqMask evalKind = iota
	// evalDict indexes a dictionary column's per-entry memo: an id load plus a byte load, with the
	// predicate called at most once per distinct entry.
	evalDict
	// evalIntMemo indexes an int column's per-value memo: a value load, a range check and a byte
	// load, with the predicate called at most once per distinct in-domain value (see maxIntMemo).
	// Placed after evalDict because a byte column is usually the more selective of the two (an id or
	// a body match) and costs a byte per row to probe rather than eight.
	evalIntMemo
	// evalSetScan tests a row's cell against the condition's sorted [fetch.Condition.AnyEqual] set:
	// a binary search per row, with the predicate called only for the members that pass. It serves
	// the byte-column forms that have no dictionary to memoize — the flat decode form and a
	// fast-pathed column's raw blob — which is where an id column (trace_id) lands and which
	// otherwise falls all the way through to [evalGeneric], one predicate call per row.
	evalSetScan
	// evalRow calls the predicate per row over a directly indexable int vector — the timestamp,
	// whose values never fall in the memo's domain.
	evalRow
	// evalGeneric calls the predicate per row through [lazyCols.colValue]: the fallback for a byte
	// column with no dictionary to memoize (the flat/fixed decode forms, or a raw fixed-width blob)
	// and for a name that resolves to no column at all.
	evalGeneric
)

// Per-entry memo states for [evalDict] and [evalIntMemo]. The zero value is "not yet evaluated", so
// a memo is reset for a new part by zeroing it.
const (
	memoUnknown uint8 = iota
	memoMatch
	memoNoMatch
)

// maxIntMemo is the exclusive upper bound of the int values [evalIntMemo] memoizes: a value in
// [0, maxIntMemo) indexes the memo directly, anything else (negative, or larger) falls through to a
// per-row predicate call. Enum-shaped columns — OTel severities (1–24), HTTP/gRPC status codes —
// live entirely inside it; timestamps, durations and byte counts live entirely outside it and pay
// only the range check.
//
// A fixed domain rather than the column's own [min, max]: deriving the real range costs a
// [simd.MinMaxInt64] pass over the whole column, which measured as a net loss (~2% on a multi-
// condition fetch) because a condition that a more selective one short-circuits ahead of it may
// never be evaluated at all — the memo must cost nothing until its first probe.
const maxIntMemo = 4096

// condEval is one condition compiled against a single part's decoded columns ([lazyCols]).
type condEval struct {
	cond *fetch.Condition
	kind evalKind

	mask []byte            // evalEqMask: the per-row bitmap
	dict *chunk.DictColumn // evalDict: the column holding the condition's cells
	memo []uint8           // evalDict/evalIntMemo: per-entry tri-state, filled lazily
	attr string            // evalDict: when set, the entry is an attrs blob and this key is looked up in it
	ints []int64           // evalIntMemo/evalRow: the column's values
	raw  rawBytesCol       // evalSetScan: the column's raw fixed-width blob, when it has one
	flat [][]byte          // evalSetScan: the flat decode form's per-row values, when it has no blob
	text []byte            // evalDict: reused rendering of an attribute value for the set test
}

// match reports whether row i satisfies this condition.
func (ce *condEval) match(lz *lazyCols, i int) bool {
	switch ce.kind {
	case evalEqMask:
		return ce.mask[i] != 0
	case evalDict:
		id := dictEntryID(ce.dict, i)
		switch ce.memo[id] {
		case memoMatch:
			return true
		case memoNoMatch:
			return false
		}

		return ce.remember(id, ce.matchEntry(ce.dict.Entries[id]))
	case evalSetScan:
		v := ce.flat
		if v == nil {
			cell, _ := ce.raw.at(i)

			return ce.setMatches(cell)
		}

		return ce.setMatches(v[i])
	case evalIntMemo:
		v := ce.ints[i]
		if uint64(v) >= maxIntMemo { // negative or out of the memoized domain
			return ce.cond.Match(signal.IntValue(v))
		}

		switch ce.memo[v] {
		case memoMatch:
			return true
		case memoNoMatch:
			return false
		}

		return ce.remember(int(v), ce.cond.Match(signal.IntValue(v)))
	case evalRow:
		return ce.cond.Match(signal.IntValue(ce.ints[i]))
	default:
		v, ok := lz.colValue(i, ce.cond.Column)
		if !ok {
			v = signal.EmptyValue()
		}

		return ce.cond.Match(v)
	}
}

// setMatches is the [evalSetScan] body: reject a cell outside the set without touching the
// predicate, verify a member through it. The set is a superset hint, not a replacement — see
// [fetch.Condition.AnyEqual] — so a member still goes through Match.
func (ce *condEval) setMatches(cell []byte) bool {
	if !fetch.InAnyEqual(ce.cond.AnyEqual, cell) {
		return false
	}

	return ce.cond.Match(signal.StringValue(cell))
}

// remember records a freshly evaluated memo slot and returns the verdict.
func (ce *condEval) remember(slot int, matched bool) bool {
	if matched {
		ce.memo[slot] = memoMatch
	} else {
		ce.memo[slot] = memoNoMatch
	}

	return matched
}

// matchEntry evaluates the predicate against one dictionary entry: the cell itself for a fixed
// column, or the value ce.attr holds inside the serialized attributes blob. An absent attribute is
// offered as [signal.EmptyValue], exactly as [recordCols.rowMatches] does — only the language knows
// whether a missing value satisfies its predicate.
func (ce *condEval) matchEntry(entry []byte) bool {
	set := ce.cond.AnyEqual

	if ce.attr == "" {
		if len(set) > 0 && !fetch.InAnyEqual(set, entry) {
			return false
		}

		return ce.cond.Match(signal.StringValue(entry))
	}

	v, found, err := signal.LookupAttribute(entry, ce.attr)
	if err != nil || !found {
		v = signal.EmptyValue()
	}

	if len(set) > 0 {
		ce.text = v.AppendText(ce.text[:0])
		if !fetch.InAnyEqual(set, ce.text) {
			return false
		}
	}

	return ce.cond.Match(v)
}

// dictEntryID returns row's dictionary id. Only valid for the dictionary decode form (IDWidth 1 or
// 2); the flat/fixed forms index Entries by row and compile to [evalGeneric] instead.
func dictEntryID(c *chunk.DictColumn, row int) int {
	if c.IDWidth == 1 {
		return int(c.IDs[row])
	}

	return int(c.IDs[row*2])<<8 | int(c.IDs[row*2+1])
}

// compileConds compiles the plan's conditions against one part's decoded columns, cheap-first. The
// returned slice and every memo in it are scratch reused across parts, so they are valid only until
// the next call.
func (p *fetchPlan) compileConds(lz *lazyCols) []condEval {
	evals := p.evalScratch[:0]
	for j := range p.conds {
		evals = append(evals, p.compileCond(lz, j))
	}

	p.evalScratch = evals

	slices.SortStableFunc(evals, func(a, b condEval) int { return cmp.Compare(a.kind, b.kind) })

	return evals
}

func (p *fetchPlan) compileCond(lz *lazyCols, j int) condEval {
	cond := &p.conds[j]
	ce := condEval{cond: cond, kind: evalGeneric}

	if len(lz.eqMask) != 0 && lz.eqMask[j] != nil {
		ce.kind, ce.mask = evalEqMask, lz.eqMask[j]

		return ce
	}

	if cond.Column == colTs {
		ce.kind, ce.ints = evalRow, lz.ts

		return ce
	}

	if ref, ok := lz.schema.ref(cond.Column); ok {
		if ref.kind == KindInt64 {
			ce.kind, ce.ints, ce.memo = evalIntMemo, lz.ints[ref.idx], p.memo(j, maxIntMemo)

			return ce
		}

		dc := lz.bytes[ref.idx]
		if dc != nil && dc.IDWidth != 0 {
			ce.kind, ce.dict, ce.memo = evalDict, dc, p.memo(j, len(dc.Entries))

			return ce
		}

		// No dictionary to memoize: a set membership still beats a per-row predicate call, over the
		// raw blob when the column has one and over the flat form's per-row values otherwise.
		if len(cond.AnyEqual) > 0 {
			switch rb := lz.rawBlob[ref.idx]; {
			case rb.blob != nil:
				ce.kind, ce.raw = evalSetScan, rb
			case dc != nil:
				ce.kind, ce.flat = evalSetScan, dc.Entries
			}
		}

		return ce
	}

	// Not a fixed column: a per-record attribute, read out of the schema's attrs blob column.
	if k, ok := lz.schema.attrsByteCol(); ok {
		if dc := lz.bytes[k]; dc != nil && dc.IDWidth != 0 {
			ce.kind, ce.dict, ce.memo, ce.attr = evalDict, dc, p.memo(j, len(dc.Entries)), cond.Column
		}
	}

	return ce
}

// memo returns condition j's zeroed per-entry memo, sized n, reusing the scratch from the previous
// part.
func (p *fetchPlan) memo(j, n int) []uint8 {
	m := slices.Grow(p.memoScratch[j][:0], n)[:n]
	clear(m)
	p.memoScratch[j] = m

	return m
}

// evalMatches reports whether row i satisfies every compiled condition (logical AND).
func evalMatches(lz *lazyCols, evals []condEval, i int) bool {
	for k := range evals {
		if !evals[k].match(lz, i) {
			return false
		}
	}

	return true
}

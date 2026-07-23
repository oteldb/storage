package recordengine

import (
	"math"
	"sort"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Fixed implicit column names (not part of the [Schema]): the int128 stream identity sort grouping
// and the int64 primary timestamp / sort key.
const (
	colStream = "stream"
	colTs     = "ts"
)

// rec is one record's values in the engine's internal form: the primary timestamp plus the int and
// byte column values in the schema's per-kind order. It is the unit the head buffers and the WAL
// frame map to, so the engine depends on no signal model.
type rec struct {
	ts    int64
	ints  []int64
	bytes [][]byte
}

// colSel is the per-column lazy-decode mask, indexed by the schema's per-kind column order. The
// timestamp is always materialized and not tracked here.
type colSel struct {
	ints  []bool
	bytes []bool
}

// fullSel selects every column (the default when a request has no projection, and the head buffer's
// always-full set).
func fullSel(s *Schema) colSel {
	sel := colSel{ints: make([]bool, s.numInts()), bytes: make([]bool, s.numBytes())}
	for k := range sel.ints {
		sel.ints[k] = true
	}

	for k := range sel.bytes {
		sel.bytes[k] = true
	}

	return sel
}

// selectColumns derives the columns a request needs: an empty projection means all columns;
// otherwise the projected columns (output) plus the conditions' columns (filter). A condition over
// a per-record attribute key (not a fixed column) needs the schema's attrs column.
func selectColumns(s *Schema, r fetch.Request) colSel {
	if len(r.Projection) == 0 {
		return fullSel(s)
	}

	sel := colSel{ints: make([]bool, s.numInts()), bytes: make([]bool, s.numBytes())}

	mark := func(name string) bool {
		if name == colTs {
			return true // the timestamp is always materialized
		}

		ref, ok := s.ref(name)
		if !ok {
			return false
		}

		if ref.kind == KindInt64 {
			sel.ints[ref.idx] = true
		} else {
			sel.bytes[ref.idx] = true
		}

		return true
	}

	for _, name := range r.Projection {
		mark(name)
	}

	for i := range r.Conditions {
		if !mark(r.Conditions[i].Column) {
			if k, ok := s.attrsByteCol(); ok { // attribute predicate reads the serialized blob
				sel.bytes[k] = true
			}
		}
	}

	return sel
}

// recordCols is a set of records laid out columnarly: the always-present ts, plus int and byte
// column vectors indexed by the schema's per-kind order. A vector is populated only when sel
// selects it (lazy decode); unselected vectors stay nil and are never touched. All populated
// vectors share one length, [recordCols.len].
type recordCols struct {
	schema *Schema
	sel    colSel
	ts     []int64
	ints   [][]int64
	bytes  []byteCol // one offsets+blob column per schema byte column (zero-value when unselected)

	// keyCache memoizes the buffer's distinct record-attribute keys (see distinctAttrKeys); it is
	// invalidated on every append. tsMin/tsMax bound the buffer's timestamps so Keys can use the
	// cache only when the query window fully covers the buffer (else a windowed scan is needed).
	keyCache      [][]byte
	keyCacheValid bool
	tsMin, tsMax  int64

	// rowScratch is a reusable kept-row index buffer for the in-place row compactions
	// (filterInPlace / trimBelow), so they stay allocation-free in a steady loop.
	rowScratch []int
	// viewBufs memoizes the per-byte-column [][]byte view slices materialized at the
	// [fetch.NamedColumn] boundary, reused across fetches when the accumulator is pooled.
	viewBufs [][][]byte
}

func (c *recordCols) len() int { return len(c.ts) }

// noteTS widens the buffer's tracked timestamp range and invalidates the key cache. Called by every
// append so distinctAttrKeys recomputes only after the rows it summarizes change.
func (c *recordCols) noteTS(ts int64) {
	if ts < c.tsMin {
		c.tsMin = ts
	}
	if ts > c.tsMax {
		c.tsMax = ts
	}
	c.keyCacheValid = false
}

// distinctAttrKeys returns the buffer's sorted distinct record-attribute keys, decoding the records
// only when the cache is stale. Keys uses this to avoid re-decoding every head record per call.
func (c *recordCols) distinctAttrKeys() [][]byte {
	if c.keyCacheValid {
		return c.keyCache
	}
	c.keyCache = distinctRecordKeys(c.schema, c)
	c.keyCacheValid = true
	return c.keyCache
}

// newRecordCols returns an empty recordCols whose selected columns are pre-sized to n rows (so the
// accumulation copies never reallocate); unselected columns stay nil.
func newRecordCols(s *Schema, n int, sel colSel) *recordCols {
	c := &recordCols{
		schema: s,
		sel:    sel,
		ts:     make([]int64, 0, n),
		ints:   make([][]int64, s.numInts()),
		bytes:  make([]byteCol, s.numBytes()),
		tsMin:  math.MaxInt64,
		tsMax:  math.MinInt64,
	}

	for k := range c.ints {
		if sel.ints[k] {
			c.ints[k] = make([]int64, 0, n)
		}
	}

	for k := range c.bytes {
		if sel.bytes[k] {
			c.bytes[k].ensure(n)
		}
	}

	return c
}

// prepare re-arms a pooled recordCols for a fresh accumulation: it adopts the new schema/selection
// and pre-sizes the selected columns to n rows, reusing the backing arrays wherever their capacity
// suffices (so a steady same-projection read loop reallocates nothing). Deselected columns are
// dropped to nil so the lazy-decode paths never touch them. It mirrors [newRecordCols] for a reused
// buffer; the byte vectors' stale element slices (aliasing the previous fetch's part bytes) are left
// to be overwritten by the coming appends — never read past the truncated length.
func (c *recordCols) prepare(s *Schema, n int, sel colSel) {
	c.schema = s
	c.sel = sel
	c.ts = ensureI64(c.ts, n)
	c.keyCache, c.keyCacheValid = nil, false
	c.tsMin, c.tsMax = math.MaxInt64, math.MinInt64

	if len(c.ints) != s.numInts() {
		c.ints = make([][]int64, s.numInts())
	}

	for k := range c.ints {
		if sel.ints[k] {
			c.ints[k] = ensureI64(c.ints[k], n)
		} else {
			c.ints[k] = nil
		}
	}

	if len(c.bytes) != s.numBytes() {
		c.bytes = make([]byteCol, s.numBytes())
	}

	for k := range c.bytes {
		if sel.bytes[k] {
			c.bytes[k].ensure(n)
		} else {
			c.bytes[k] = byteCol{}
		}
	}
}

// ensureI64 returns s truncated to length 0 if it already has capacity for n, else a fresh slice
// pre-sized to n. Reused for the timestamp and every int column when re-arming a pooled buffer.
func ensureI64(s []int64, n int) []int64 {
	if cap(s) >= n {
		return s[:0]
	}

	// At least doubling, so a reused buffer whose shape creeps up by a few rows per round does not
	// reallocate every round (see [byteCol.ensureBytes]).
	return make([]int64, 0, max(n, 2*cap(s)))
}

// byteSize returns the in-flight memory the buffer holds: its timestamps, int columns, and the
// blob bytes of its byte columns (the basis for the head's MaxInFlightBytes accounting after a trim).
func (c *recordCols) byteSize() int64 {
	n := int64(8 * len(c.ts))
	for k := range c.ints {
		n += int64(8 * len(c.ints[k]))
	}

	for k := range c.bytes {
		n += c.bytes[k].byteSize()
	}

	return n
}

// appendRow appends row i of src's selected columns (ts always). Byte cells are copied into c's
// blob (they no longer alias src). src must populate at least c's selected columns.
func (c *recordCols) appendRow(src *recordCols, i int) {
	c.ts = append(c.ts, src.ts[i])
	c.noteTS(src.ts[i])

	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = append(c.ints[k], src.ints[k][i])
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k].appendCell(src.bytes[k].at(i))
		}
	}
}

// appendRange bulk-appends rows [lo, hi) of src's selected columns — one append per column.
func (c *recordCols) appendRange(src *recordCols, lo, hi int) {
	c.ts = append(c.ts, src.ts[lo:hi]...)
	for _, t := range src.ts[lo:hi] {
		c.noteTS(t)
	}

	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = append(c.ints[k], src.ints[k][lo:hi]...)
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k].appendRange(&src.bytes[k], lo, hi)
		}
	}
}

// keep retains only rows [lo, hi) of every selected column (ts always), discarding the rest in
// place — the slice dual of [recordCols.appendRange]. It reslices the existing backing arrays (no
// copy), so a pooled buffer's capacity is preserved for reuse. Used by the fetch limit pushdown to
// trim a ts-sorted accumulator to the rows that survive the global top-N boundary.
func (c *recordCols) keep(lo, hi int) {
	c.ts = c.ts[lo:hi]

	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = c.ints[k][lo:hi]
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k].keep(lo, hi)
		}
	}
}

// appendClone appends r to a full (head) buffer. appendCell copies each byte cell into the column's
// blob, so the head owns its bytes (it outlives the caller's batch). Every column is populated —
// head buffers always carry the full schema.
func (c *recordCols) appendClone(r rec) {
	c.ts = append(c.ts, r.ts)
	c.noteTS(r.ts)
	for k := range c.ints {
		c.ints[k] = append(c.ints[k], r.ints[k])
	}

	for k := range c.bytes {
		c.bytes[k].appendCell(r.bytes[k])
	}
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// sortByTs reorders every selected column by ascending timestamp (stable). Records arrive
// part-ordered and a part's rows are ts-sorted, so the accumulated window is very often already
// ordered — an O(n) check skips the O(n log n) sort and its permute allocations.
func (c *recordCols) sortByTs() {
	if c.isSortedByTs() {
		return
	}

	idx := make([]int, c.len())
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool { return c.ts[idx[a]] < c.ts[idx[b]] })

	c.ts = permute(c.ts, idx)
	for k := range c.ints {
		if c.sel.ints[k] {
			c.ints[k] = permute(c.ints[k], idx)
		}
	}

	for k := range c.bytes {
		if c.sel.bytes[k] {
			c.bytes[k] = permuteBytes(&c.bytes[k], idx)
		}
	}
}

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

// toBatch builds the fetch batch for one stream, materializing only the projected columns (an
// empty projection materializes all). Timestamps always carries each record's primary time.
func (c *recordCols) toBatch(id signal.SeriesID, s signal.Series, projection []string) *fetch.Batch {
	return &fetch.Batch{
		ID:         id,
		Series:     s,
		Timestamps: c.ts,
		Columns:    c.projectColumns(projection),
	}
}

// projectColumns returns the named columns to output (all columns when projection is empty). A byte
// column is materialized to the [fetch.NamedColumn] [][]byte shape as zero-copy views into its blob
// (see [recordCols.byteViews]); the views share the accumulator's lifetime via the recycle contract.
func (c *recordCols) projectColumns(projection []string) []fetch.NamedColumn {
	if len(projection) == 0 {
		out := make([]fetch.NamedColumn, 0, c.schema.numInts()+c.schema.numBytes())
		for k := range c.ints {
			out = append(out, fetch.NamedColumn{Name: c.schema.intColumn(k).Name, Int64: c.ints[k]})
		}

		for k := range c.bytes {
			out = append(out, fetch.NamedColumn{Name: c.schema.byteColumn(k).Name, Bytes: c.byteViews(k)})
		}

		return out
	}

	out := make([]fetch.NamedColumn, 0, len(projection))
	for _, name := range projection {
		ref, ok := c.schema.ref(name)
		if !ok {
			continue
		}

		if ref.kind == KindInt64 {
			out = append(out, fetch.NamedColumn{Name: name, Int64: c.ints[ref.idx]})
		} else {
			out = append(out, fetch.NamedColumn{Name: name, Bytes: c.byteViews(ref.idx)})
		}
	}

	return out
}

// byteViews materializes byte column k as a [][]byte of zero-copy views into its blob, reusing the
// per-column view buffer across fetches when the accumulator is pooled.
func (c *recordCols) byteViews(k int) [][]byte {
	if len(c.viewBufs) != len(c.bytes) {
		c.viewBufs = make([][][]byte, len(c.bytes))
	}

	c.viewBufs[k] = c.bytes[k].views(c.viewBufs[k])

	return c.viewBufs[k]
}

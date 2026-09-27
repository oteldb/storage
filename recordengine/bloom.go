package recordengine

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"

	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// A column's per-part bloom is written to "{prefix}/bloom-{column}.bin" and consulted to prune the
// whole part when a predicate's required token is proven absent (no false negatives; surviving
// parts are still re-checked per row).
func bloomKey(prefix, column string) string { return prefix + "/bloom-" + column + ".bin" }

// attrToken builds the key-scoped token key ‖ 0x00 ‖ value used by [BloomAttrs] columns (and the
// query side). A separator collision can only cause a false positive (an extra scan).
func attrToken(key, value []byte) []byte {
	return appendAttrToken(make([]byte, 0, len(key)+1+len(value)), key, value)
}

// appendAttrToken is [attrToken] appending into a caller-owned buffer, so a build that emits one
// token per attribute per row reuses a single allocation.
func appendAttrToken(dst, key, value []byte) []byte {
	dst = append(dst, key...)
	dst = append(dst, 0x00)

	return append(dst, value...)
}

// tokenizer holds the scratch a per-value token walk reuses, so walking a column allocates nothing per
// token or per row. It is the one definition of what a column contributes to its bloom, shared by the
// flush build ([bloomBuilder]) and the merge's streamed one ([bloomAccum]), which must produce the
// same filter.
type tokenizer struct {
	words bloom.Scanner     // token scanner, keeps its case-folding buffer
	attrs signal.Attributes // reused attribute-decode buffer
	text  []byte            // reused rendered attribute value
	token []byte            // reused key-scoped token
}

// tokens calls fn once per bloom token of val under mode:
//   - FullText: a token per lowercased word.
//   - Equality: val verbatim, unless it is empty — the empty value is never an equality lookup target.
//   - Attrs: per attribute of the serialized blob, the equality token key‖value and a key‖word token
//     per word of the rendered value. A blob that fails to decode yields nothing.
//
// Tokens alias the tokenizer's scratch and are invalid after fn returns.
func (t *tokenizer) tokens(mode BloomMode, val []byte, fn func(token []byte)) {
	switch mode {
	case BloomFullText:
		t.words.Reset(val)
		for {
			tok, ok := t.words.Next()
			if !ok {
				return
			}

			fn(tok)
		}
	case BloomEquality:
		if len(val) > 0 {
			fn(val)
		}
	case BloomAttrs:
		a, _, err := signal.AppendAttributes(t.attrs[:0], val)
		if err != nil {
			return
		}

		t.attrs = a
		for j := range a {
			t.text = a[j].Value.AppendText(t.text[:0])

			t.token = appendAttrToken(t.token[:0], a[j].Key, t.text)
			fn(t.token)

			// The rendered text is scanned in place; the key-scoped token is rebuilt per word into
			// the same buffer, which fn has finished with by then.
			t.words.Reset(t.text)
			for {
				w, ok := t.words.Next()
				if !ok {
					break
				}

				t.token = appendAttrToken(t.token[:0], a[j].Key, w)
				fn(t.token)
			}
		}
	case BloomNone:
	}
}

// count returns how many tokens [tokenizer.tokens] emits for val, scoring a whole value at a time
// ([bloom.CountTokens]) rather than materializing its tokens.
func (t *tokenizer) count(mode BloomMode, val []byte) int {
	switch mode {
	case BloomFullText:
		return bloom.CountTokens(val)
	case BloomEquality:
		if len(val) > 0 {
			return 1
		}
	case BloomAttrs:
		a, _, err := signal.AppendAttributes(t.attrs[:0], val)
		if err != nil {
			return 0
		}

		t.attrs = a

		n := 0
		for j := range a {
			t.text = a[j].Value.AppendText(t.text[:0])
			n += 1 + bloom.CountTokens(t.text)
		}

		return n
	case BloomNone:
	}

	return 0
}

// bloomBuilder holds the reusable scratch one column's bloom build needs. Since the scratch outlives a
// single column ([bloomBuilder.build] re-arms it), a part's blooms allocate nothing per column either.
// One builder is owned by the engine and re-armed per column, exactly like flushColumns: see
// [Engine.blooms].
type bloomBuilder struct {
	tokenizer

	distinct bloom.Sketch        // reused distinct-token estimator (constant 4 KiB)
	seen     map[uint64]struct{} // value hashes already walked, for the repeated-value skip
	rowsBuf  []int               // backing array of rows, kept across columns
	rows     []int               // rows holding a value's first occurrence; nil ⇒ walk every row
}

// Per-part filters are consulted once per part, so a query over a store with thousands of parts
// pays the false-positive rate that many times: at p=0.01 and 200 parts a token absent from the
// whole store still scans ~2 parts. A filter's size is only logarithmic in p (bits per item =
// -ln p / ln²2 ⇒ 9.6 at 1e-2, 14.4 at 1e-3, 28.8 at 1e-6), so pruning-critical columns buy near
// exact pruning cheaply once the filter is sized by DISTINCT tokens ([bloomBuilder.distinctTokens]):
//
//   - Equality (trace_id): few distinct values per part (thousands) and the lookup that must not
//     touch an irrelevant part at all — 1e-6 costs single-digit KiB.
//   - FullText / Attrs: tens to hundreds of thousands of distinct tokens per part; 1e-3 keeps the
//     expected false-positive parts well under one for a realistic part count.
func falsePositiveRate(mode BloomMode) float64 {
	if mode == BloomEquality {
		return 1e-6
	}

	return 1e-2
}

// smallFilterBytes is the size below which a filter is left sized by token *occurrences* rather
// than by distinct tokens. Blooms are resident per live part, so what matters is bytes × part
// count: parts are capped at MaxPartBytes (64 MiB by default), so a 32 KiB filter costs ≈0.05% of
// the data it indexes no matter how large the store grows. Under that, paying a second pass over
// the column to size it exactly would cost more ingest CPU than the bytes are worth; over it, the
// repetition factor (60–340× on real log text) is what turns blooms into the process's largest
// resident term, and the pass pays for itself many times over.
const smallFilterBytes = 32 << 10

// countingWorthwhileBytes is the column size above which the occurrence count is skipped: a column
// this large only implies a small filter when its tokens average tens of bytes each, which log text
// and attribute values do not, so the filter is sized by the distinct estimate directly.
const countingWorthwhileBytes = 1 << 20

// filterItems is the item count a column's filter is sized for, from the column's total value bytes,
// its token occurrences (consulted only for a column of at most countingWorthwhileBytes) and its
// distinct-token estimate. When the filter the occurrences imply is already small in absolute terms,
// that is the answer — an oversized-but-tiny filter is simply a lower false-positive rate than asked
// for.
func filterItems(mode BloomMode, bytes int64, occurrences, distinct func() int) int {
	if bytes > countingWorthwhileBytes {
		return distinct()
	}

	n := occurrences()
	if bloom.Bits(n, falsePositiveRate(mode))/8 <= smallFilterBytes {
		return n
	}

	return distinct()
}

// sizeTokens returns the item count to size the column's filter for ([filterItems]). The occurrence
// count is one cheap pass ([bloom.CountTokens] scores a whole value at a time, no tokens
// materialized, no hashing); only when it does not settle the size does the builder walk the column a
// second time for the distinct estimate.
func (bb *bloomBuilder) sizeTokens(mode BloomMode, values *byteCol) int {
	return filterItems(mode, values.byteSize(),
		func() int { return bb.countTokens(mode, values) },
		func() int { return bb.distinctTokens(mode, values) })
}

// distinctTokens estimates how many DISTINCT tokens [bloomBuilder.forEachToken] emits for the
// column — the count [bloom.New] must be sized by once the filter is big enough to matter. It walks
// the same token stream forEachToken does, so the two cannot drift; the estimate costs one hash per
// token and constant space.
func (bb *bloomBuilder) distinctTokens(mode BloomMode, values *byteCol) int {
	bb.distinct.Reset()
	bb.forEachToken(mode, values, bb.distinct.Add)

	return bb.distinct.Estimate()
}

// countTokens returns how many tokens [bloomBuilder.forEachToken] emits for the column, counting
// per row rather than per token.
//
// It must stay in step with forEachToken: it decides both the small-filter shortcut and, when taken,
// the filter's size. TestBuildColumnBloomMatchesReference / FuzzBuildColumnBloomMatchesReference
// detect any drift — they compare against a single-pass build that counts by materializing.
func (bb *bloomBuilder) countTokens(mode BloomMode, values *byteCol) int {
	n := 0

	if mode == BloomEquality {
		// Straight-line rather than through eachValue: the body is a length test, so a closure call
		// per row would dominate it. This is the column shape (trace ids, one short value per row)
		// whose count actually runs — the larger columns skip it entirely.
		for i := range values.rows() {
			if len(values.at(i)) > 0 {
				n++
			}
		}

		return n
	}

	eachValue(values, nil, func(val []byte) { n += bb.count(mode, val) })

	return n
}

// forEachToken calls fn once per bloom token of the column under mode. Tokens passed to fn alias
// the builder's scratch and are invalid after fn returns.
//
// Rows whose value was already walked are skipped ([bloomBuilder.markRows]): a bloom is a set, so
// re-walking a repeated value re-derives tokens that are already in it — and log columns repeat
// heavily (templated bodies, one attribute blob per stream). The filter is bit-identical either way.
func (bb *bloomBuilder) forEachToken(mode BloomMode, values *byteCol, fn func(token []byte)) {
	switch mode {
	case BloomFullText, BloomAttrs:
		eachValue(values, bb.rows, func(val []byte) { bb.tokens(mode, val, fn) })
	case BloomEquality:
		eachEquality(values, bb.rows, fn)
	case BloomNone:
	}
}

// maxDedupRows caps the first-occurrence set at 256k values (a few MiB of map, transient inside one
// column build). Past it the remaining rows are all kept: the set stays correct (every distinct
// value is still walked), it just stops growing.
const maxDedupRows = 1 << 18

// selectRows decides which rows the build walks. The dedup pass costs a hash and a map probe per
// row, so it is only run for the modes whose columns actually repeat: log bodies are templated and
// one attribute blob is shared by a whole stream, so skipping duplicates there removes most of the
// tokenization work. [BloomEquality] columns are the opposite — trace_id, where real data is ~82%
// distinct — so the pass would pay a full-column hash to skip almost nothing.
//
// Which rows are walked never changes the filter: a repeated value re-derives tokens the filter and
// the distinct-count sketch already hold, and both are idempotent per token (see
// TestBuildColumnBloomDedupIsBitIdentical).
func (bb *bloomBuilder) selectRows(mode BloomMode, values *byteCol) {
	if mode == BloomEquality {
		bb.rows = nil // walk every row

		return
	}

	bb.markRows(values)
}

// markRows fills bb.rows with the rows holding a value's first occurrence. Values are compared by
// 64-bit hash: a collision would drop a row (a marginally smaller token set, never a false
// negative for the values that were walked), at a probability far below the filter's own.
// It is the dominant cost of a heavily repeated column's build: one hash and one map probe per row,
// against a token walk over only the distinct values that survive.
func (bb *bloomBuilder) markRows(values *byteCol) {
	if bb.seen == nil {
		bb.seen = make(map[uint64]struct{}, 1024)
	}

	clear(bb.seen)

	rows := bb.rowsBuf[:0]

	n := values.rows()
	for i := range n {
		h := xxh3.Hash(values.at(i))
		if _, dup := bb.seen[h]; dup {
			continue
		}

		if len(bb.seen) >= maxDedupRows {
			// The set is full: keep every remaining row rather than dropping the dedup entirely,
			// so the rows walked so far still skip their duplicates.
			for ; i < n; i++ {
				rows = append(rows, i)
			}

			break
		}

		bb.seen[h] = struct{}{}
		rows = append(rows, i)
	}

	bb.rowsBuf, bb.rows = rows, rows
}

// eachValue calls fn with each selected row's value (rows nil ⇒ every row).
func eachValue(b *byteCol, rows []int, fn func(val []byte)) {
	if rows == nil {
		for i := range b.rows() {
			fn(b.at(i))
		}

		return
	}

	for _, i := range rows {
		fn(b.at(i))
	}
}

// eachEquality emits each non-empty value verbatim, as [tokenizer.tokens] does. Its per-row body is
// a length test, so it walks the column directly rather than through [eachValue]'s closure: one
// indirect call per row instead of two.
func eachEquality(values *byteCol, rows []int, fn func(token []byte)) {
	if rows == nil {
		for i := range values.rows() {
			if v := values.at(i); len(v) > 0 {
				fn(v)
			}
		}

		return
	}

	for _, i := range rows {
		if v := values.at(i); len(v) > 0 {
			fn(v)
		}
	}
}

// build builds the bloom for one bloom-bearing column over its per-record values ([tokenizer.tokens]
// says which tokens each mode takes).
//
// The column is walked twice — once to estimate the distinct token count [bloom.New] must be sized
// by, once to hash the tokens in — rather than materializing every token to learn that count. Both
// passes see the same token set, so the filter matches a single-pass build; the second walk is far
// cheaper than the per-token allocations (and the live [][]byte holding them) it replaces.
func (bb *bloomBuilder) build(mode BloomMode, values *byteCol) []byte {
	if mode == BloomNone {
		return nil
	}

	// The row selection is computed once and drives both the sizing walk and the filling one.
	bb.selectRows(mode, values)

	f := bloom.New(bb.sizeTokens(mode, values), falsePositiveRate(mode))
	bb.forEachToken(mode, values, f.Add)

	return f.Encode(nil)
}

// buildColumnBloom is [bloomBuilder.build] on a throwaway builder, for callers with a single column
// to build and no builder to re-arm.
func buildColumnBloom(mode BloomMode, values *byteCol) []byte {
	var bb bloomBuilder

	return bb.build(mode, values)
}

// writeBlooms writes a bloom sidecar for each bloom-bearing column of the schema, over the flushed
// columns. The blooms share the part prefix, so deletePart / Reset clean them up. bb is re-armed per
// column, so a part's whole bloom set is built out of one set of scratch buffers.
func writeBlooms(
	ctx context.Context, b backend.Backend, schema *Schema, prefix string, cols *recordCols, bb *bloomBuilder,
) error {
	for k := range schema.byteCols {
		col := schema.byteColumn(k)
		if col.Bloom == BloomNone {
			continue
		}

		data := bb.build(col.Bloom, &cols.bytes[k])
		if err := backend.WriteDeferred(ctx, b, bloomKey(prefix, col.Name), data); err != nil {
			return errors.Wrapf(err, "write bloom %q", col.Name)
		}
	}

	return nil
}

// blooms returns the engine's reusable bloom builder, allocating it on the first part written. Every
// part write goes through a flush or a merge, and both hold flushMu for their whole body, so the
// builder has one user at a time — the same ownership as [Engine.flushBuf]; call it under flushMu.
// It keeps its scratch (the token/attribute buffers and the first-occurrence map, bounded by
// maxDedupRows) between parts, which is the point: otherwise every bloom-bearing column of every
// flush reallocates all of it.
func (e *Engine) blooms() *bloomBuilder {
	if e.bloomBuf == nil {
		e.bloomBuf = &bloomBuilder{}
	}

	return e.bloomBuf
}

// loadBlooms reads the bloom sidecar of each bloom-bearing column. A missing *or corrupt* sidecar
// is skipped (that column simply is not prunable — the part is always scanned for it). The bloom
// only ever removes parts a per-row re-check would have removed anyway, so dropping one widens the
// scan and never the result; a corrupt one is logged, not fatal, since it must not take the whole
// prefix offline.
func loadBlooms(
	ctx context.Context, b backend.Backend, schema *Schema, prefix string, corrupt *obs.Corruption,
) (map[string]*bloom.Filter, error) {
	var out map[string]*bloom.Filter

	for k := range schema.byteCols {
		col := schema.byteColumn(k)
		if col.Bloom == BloomNone {
			continue
		}

		data, err := b.Read(ctx, bloomKey(prefix, col.Name))
		if err != nil {
			if errors.Is(err, backend.ErrNotExist) {
				continue
			}

			return nil, errors.Wrapf(err, "read bloom %q", col.Name)
		}

		f, _, err := bloom.Decode(data)
		if err != nil {
			zctx.From(ctx).Error("corrupt bloom sidecar, part is not prunable by this column",
				zap.String("part", prefix), zap.String("column", col.Name), zap.Error(err))
			corrupt.Detected(ctx, "bloom", obs.CorruptTolerated)

			continue
		}

		if out == nil {
			out = make(map[string]*bloom.Filter)
		}

		out[col.Name] = f
	}

	return out, nil
}

// mayContain reports whether the part can hold a record satisfying every condition's serializable
// hint — false ⇒ a bloom proved a required equality value or full-text/attribute token absent, so
// the part is pruned. Conditions whose column has no bloom (or no hint) never prune.
func (p *part) mayContain(conds []fetch.Condition) bool {
	for i := range conds {
		if !p.conditionMayMatch(&conds[i]) {
			return false
		}
	}

	return true
}

func (p *part) conditionMayMatch(c *fetch.Condition) bool {
	if _, ok := p.schema.ref(c.Column); !ok {
		return p.attrConditionMayMatch(c)
	}

	// Fixed-column condition: consult that column's bloom (FullText tokens or Equality value).
	f := p.blooms[c.Column]
	if f == nil {
		return true
	}

	for _, tok := range c.Tokens {
		if !f.Test(tok) {
			return false
		}
	}

	// An equality hint may only be tested against a filter that holds whole values. A
	// [BloomFullText] column holds the *tokens* of each value, so a multi-token value it does hold
	// would test absent and prune a part that matches; such a condition prunes by its Tokens above.
	// The empty value is skipped by the Equality build (see eachEquality), so it is never provably
	// absent either.
	ref, _ := p.schema.ref(c.Column) // present: the no-such-column case returned above
	if p.schema.byteColumn(ref.idx).Bloom != BloomEquality {
		return true
	}

	if c.Equal != nil && c.Equal.Value != "" && !f.Test([]byte(c.Equal.Value)) {
		return false
	}

	return anyTokenPresent(f, c.AnyEqual, nil)
}

// attrConditionMayMatch is [part.conditionMayMatch] for a column the schema does not carry: a
// per-record attribute, consulted against the Attrs-column bloom with key-scoped tokens.
func (p *part) attrConditionMayMatch(c *fetch.Condition) bool {
	k, has := p.schema.attrsByteCol()
	if !has {
		return true
	}

	f := p.blooms[p.schema.byteColumn(k).Name]
	if f == nil {
		return true
	}

	if c.Equal != nil && !f.Test(attrToken([]byte(c.Equal.Name), []byte(c.Equal.Value))) {
		return false
	}

	key := []byte(c.Column)
	if !anyTokenPresent(f, c.AnyEqual, func(v []byte) []byte { return attrToken(key, v) }) {
		return false
	}

	for _, tok := range c.Tokens {
		if !f.Test(attrToken(key, tok)) {
			return false
		}
	}

	return true
}

// anyTokenPresent is the set-membership prune test for [fetch.Condition.AnyEqual]: the part
// survives when *any* member may be present, and is pruned only when the filter proves every one
// absent. That disjunction is why the set cannot be passed as N separate conditions —
// [part.mayContain] ANDs across conditions.
//
// token maps a member to the bloom token to test (nil ⇒ the member verbatim, for a fixed equality
// column). An empty member is never provably absent: the equality build skips empty values
// ([eachEquality]), so it always keeps the part.
func anyTokenPresent(f *bloom.Filter, set [][]byte, token func([]byte) []byte) bool {
	if len(set) == 0 {
		return true // no hint
	}

	for _, v := range set {
		if len(v) == 0 {
			return true
		}

		if token != nil {
			v = token(v)
		}

		if f.Test(v) {
			return true
		}
	}

	return false
}

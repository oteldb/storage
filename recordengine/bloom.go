package recordengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/zeebo/xxh3"

	"github.com/oteldb/storage/backend"

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

// bloomBuilder holds the reusable scratch one column's bloom build needs, so walking a column
// allocates nothing per token or per row.
type bloomBuilder struct {
	words    bloom.Scanner       // token scanner, keeps its case-folding buffer
	attrs    signal.Attributes   // reused attribute-decode buffer
	text     []byte              // reused rendered attribute value
	token    []byte              // reused key-scoped token
	distinct bloom.Sketch        // reused distinct-token estimator (constant 4 KiB)
	seen     map[uint64]struct{} // value hashes already walked, for the repeated-value skip
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

// sizeTokens returns the item count to size the column's filter for.
//
// [bloomBuilder.countTokens] counts occurrences in one cheap pass ([bloom.CountTokens] scores a
// whole value at a time, no tokens materialized, no hashing). When the filter that count implies is
// already small in absolute terms, that is the answer — an oversized-but-tiny filter is simply a
// lower false-positive rate than asked for. Only when it is not small does the builder walk the
// column a second time to estimate the DISTINCT count the filter should really be sized by.
func (bb *bloomBuilder) sizeTokens(mode BloomMode, values *byteCol) int {
	// Above this the counting pass is wasted work: a column this large only implies a small filter
	// when its tokens average tens of bytes each, which log text and attribute values do not, so go
	// straight to the distinct estimate and keep the build at two passes over the column.
	const countingWorthwhileBytes = 1 << 20

	if len(values.data) > countingWorthwhileBytes {
		return bb.distinctTokens(mode, values)
	}

	occurrences := bb.countTokens(mode, values)
	if bloom.Bits(occurrences, falsePositiveRate(mode))/8 <= smallFilterBytes {
		return occurrences
	}

	return bb.distinctTokens(mode, values)
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
// per row rather than per token — [bloom.CountTokens] scores a whole value in one call, where
// counting through forEachToken would pay an indirect call per token.
//
// It must stay in step with forEachToken: it decides both the small-filter shortcut and, when taken,
// the filter's size. TestBuildColumnBloomMatchesReference / FuzzBuildColumnBloomMatchesReference
// detect any drift — they compare against a single-pass build that counts by materializing.
func (bb *bloomBuilder) countTokens(mode BloomMode, values *byteCol) int {
	n := 0

	switch mode {
	case BloomFullText:
		for i := range values.rows() {
			n += bloom.CountTokens(values.at(i))
		}
	case BloomEquality:
		for i := range values.rows() {
			if len(values.at(i)) > 0 {
				n++
			}
		}
	case BloomAttrs:
		for i := range values.rows() {
			a, _, err := signal.AppendAttributes(bb.attrs[:0], values.at(i))
			if err != nil {
				continue
			}

			bb.attrs = a
			for j := range a {
				// One key‖value token per attribute, plus one key‖word token per word.
				bb.text = a[j].Value.AppendText(bb.text[:0])
				n += 1 + bloom.CountTokens(bb.text)
			}
		}
	case BloomNone:
	}

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
	case BloomFullText:
		bb.eachFullText(values, fn)
	case BloomEquality:
		eachEquality(values, bb.rows, fn)
	case BloomAttrs:
		bb.eachAttrs(values, fn)
	case BloomNone:
	}
}

// maxDedupRows caps the first-occurrence set at 256k values (a few MiB of map, transient inside one
// column build). Past it the remaining rows are all kept: the set stays correct (every distinct
// value is still walked), it just stops growing.
const maxDedupRows = 1 << 18

// markRows fills bb.rows with the rows holding a value's first occurrence, or leaves it nil when the
// column has too many distinct values for the dedup to be worth its map. Values are compared by
// 64-bit hash: a collision would drop a row (a marginally smaller token set, never a false
// negative for the values that were walked), at a probability far below the filter's own.
func (bb *bloomBuilder) markRows(values *byteCol) {
	bb.rows = bb.rows[:0]

	if bb.seen == nil {
		bb.seen = make(map[uint64]struct{}, 1024)
	}

	clear(bb.seen)

	for i := range values.rows() {
		h := xxh3.Hash(values.at(i))
		if _, dup := bb.seen[h]; dup {
			continue
		}

		if len(bb.seen) >= maxDedupRows {
			// The set is full: keep every remaining row rather than dropping the dedup entirely,
			// so the rows walked so far still skip their duplicates.
			for ; i < values.rows(); i++ {
				bb.rows = append(bb.rows, i)
			}

			return
		}

		bb.seen[h] = struct{}{}
		bb.rows = append(bb.rows, i)
	}
}

// each walks the rows the builder selected: the first-occurrence set when markRows built one, every
// row otherwise.
func (bb *bloomBuilder) each(values *byteCol, fn func(i int)) {
	eachRow(values, bb.rows, fn)
}

func eachRow(values *byteCol, rows []int, fn func(i int)) {
	if rows == nil {
		for i := range values.rows() {
			fn(i)
		}

		return
	}

	for _, i := range rows {
		fn(i)
	}
}

// eachFullText emits a token per lowercased word of each value.
func (bb *bloomBuilder) eachFullText(values *byteCol, fn func(token []byte)) {
	bb.each(values, func(i int) {
		bb.words.Reset(values.at(i))
		for {
			tok, ok := bb.words.Next()
			if !ok {
				break
			}

			fn(tok)
		}
	})
}

// eachEquality emits each non-empty value verbatim. Empty values (e.g. a log record with no
// trace_id) are skipped: they are never an equality lookup target, and indexing them would size the
// filter to the row count and hash a value per row for nothing — the dominant cost when a column is
// mostly empty.
func eachEquality(values *byteCol, rows []int, fn func(token []byte)) {
	eachRow(values, rows, func(i int) {
		if v := values.at(i); len(v) > 0 {
			fn(v)
		}
	})
}

// eachAttrs emits, per attribute of each serialized blob, the equality token key‖value and a
// key‖word token per word of the rendered value. A blob that fails to decode is skipped.
func (bb *bloomBuilder) eachAttrs(values *byteCol, fn func(token []byte)) {
	bb.each(values, func(i int) {
		a, _, err := signal.AppendAttributes(bb.attrs[:0], values.at(i))
		if err != nil {
			return
		}

		bb.attrs = a
		for j := range a {
			bb.text = a[j].Value.AppendText(bb.text[:0])

			bb.token = appendAttrToken(bb.token[:0], a[j].Key, bb.text)
			fn(bb.token)

			// The rendered text is scanned in place; the key-scoped token is rebuilt per word into
			// the same buffer, which fn has finished with by then.
			bb.words.Reset(bb.text)
			for {
				w, ok := bb.words.Next()
				if !ok {
					break
				}

				bb.token = appendAttrToken(bb.token[:0], a[j].Key, w)
				fn(bb.token)
			}
		}
	})
}

// buildColumnBloom builds the bloom for one bloom-bearing column over its per-record values.
//   - FullText: a token per lowercased word of each value.
//   - Equality: each value verbatim (exact-match pruning, e.g. trace-by-id).
//   - Attrs: per attribute (k,v) of each serialized blob, the equality token k‖v and a full-text
//     token k‖word per value word.
//
// The column is walked twice — once to estimate the distinct token count [bloom.New] must be sized
// by, once to hash the tokens in — rather than materializing every token to learn that count. Both
// passes see the same token set, so the filter matches a single-pass build; the second walk is far
// cheaper than the per-token allocations (and the live [][]byte holding them) it replaces.
func buildColumnBloom(mode BloomMode, values *byteCol) []byte {
	if mode == BloomNone {
		return nil
	}

	var bb bloomBuilder

	// The first-occurrence set is computed once and drives both the sizing walk and the filling one.
	bb.markRows(values)

	f := bloom.New(bb.sizeTokens(mode, values), falsePositiveRate(mode))
	bb.forEachToken(mode, values, f.Add)

	return f.Encode(nil)
}

// writeBlooms writes a bloom sidecar for each bloom-bearing column of the schema, over the flushed
// columns. The blooms share the part prefix, so deletePart / Reset clean them up.
func writeBlooms(ctx context.Context, b backend.Backend, schema *Schema, prefix string, cols *recordCols) error {
	for k := range schema.byteCols {
		col := schema.byteColumn(k)
		if col.Bloom == BloomNone {
			continue
		}

		data := buildColumnBloom(col.Bloom, &cols.bytes[k])
		if err := b.Write(ctx, bloomKey(prefix, col.Name), data); err != nil {
			return errors.Wrapf(err, "write bloom %q", col.Name)
		}
	}

	return nil
}

// loadBlooms reads the bloom sidecar of each bloom-bearing column. A missing sidecar is skipped
// (that column simply is not prunable — the part is always scanned for it).
func loadBlooms(ctx context.Context, b backend.Backend, schema *Schema, prefix string) (map[string]*bloom.Filter, error) {
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
			return nil, errors.Wrapf(err, "decode bloom %q", col.Name)
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
	// Attribute condition (column is not a fixed schema column): consult the Attrs-column bloom
	// with key-scoped tokens.
	if _, ok := p.schema.ref(c.Column); !ok {
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

		for _, tok := range c.Tokens {
			if !f.Test(attrToken([]byte(c.Column), tok)) {
				return false
			}
		}

		return true
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
	if c.Equal != nil && c.Equal.Value != "" && p.schema.byteColumn(ref.idx).Bloom == BloomEquality &&
		!f.Test([]byte(c.Equal.Value)) {
		return false
	}

	return true
}

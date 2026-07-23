package recordengine

import (
	"context"

	"github.com/go-faster/errors"

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
	words bloom.Scanner     // token scanner, keeps its case-folding buffer
	attrs signal.Attributes // reused attribute-decode buffer
	text  []byte            // reused rendered attribute value
	token []byte            // reused key-scoped token
}

// forEachToken calls fn once per bloom token of the column under mode.
//
// [buildColumnBloom] runs it twice — once to count, once to fill — so keeping the token set in one
// place is what guarantees the two passes agree; a filter sized by one walk and filled by a
// different one would silently over- or under-saturate. Tokens passed to fn alias the builder's
// scratch and are invalid after fn returns.
func (bb *bloomBuilder) forEachToken(mode BloomMode, values *byteCol, fn func(token []byte)) {
	switch mode {
	case BloomFullText:
		bb.eachFullText(values, fn)
	case BloomEquality:
		eachEquality(values, fn)
	case BloomAttrs:
		bb.eachAttrs(values, fn)
	case BloomNone:
	}
}

// eachFullText emits a token per lowercased word of each value.
func (bb *bloomBuilder) eachFullText(values *byteCol, fn func(token []byte)) {
	for i := range values.rows() {
		bb.words.Reset(values.at(i))
		for {
			tok, ok := bb.words.Next()
			if !ok {
				break
			}

			fn(tok)
		}
	}
}

// eachEquality emits each non-empty value verbatim. Empty values (e.g. a log record with no
// trace_id) are skipped: they are never an equality lookup target, and indexing them would size the
// filter to the row count and hash a value per row for nothing — the dominant cost when a column is
// mostly empty.
func eachEquality(values *byteCol, fn func(token []byte)) {
	for i := range values.rows() {
		if v := values.at(i); len(v) > 0 {
			fn(v)
		}
	}
}

// eachAttrs emits, per attribute of each serialized blob, the equality token key‖value and a
// key‖word token per word of the rendered value. A blob that fails to decode is skipped.
func (bb *bloomBuilder) eachAttrs(values *byteCol, fn func(token []byte)) {
	for i := range values.rows() {
		a, _, err := signal.AppendAttributes(bb.attrs[:0], values.at(i))
		if err != nil {
			continue
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
	}
}

// buildColumnBloom builds the bloom for one bloom-bearing column over its per-record values.
//   - FullText: a token per lowercased word of each value.
//   - Equality: each value verbatim (exact-match pruning, e.g. trace-by-id).
//   - Attrs: per attribute (k,v) of each serialized blob, the equality token k‖v and a full-text
//     token k‖word per value word.
//
// The column is walked twice — once to count tokens, once to hash them — rather than materializing
// every token to learn the count [bloom.New] needs. Both passes see the same token set, so the
// filter is byte-identical to a single-pass build; the second walk is far cheaper than the
// per-token allocations (and the live [][]byte holding them) it replaces.
func buildColumnBloom(mode BloomMode, values *byteCol) []byte {
	if mode == BloomNone {
		return nil
	}

	var bb bloomBuilder

	n := 0
	bb.forEachToken(mode, values, func([]byte) { n++ })

	f := bloom.New(n, 0.01)
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

	if c.Equal != nil && !f.Test([]byte(c.Equal.Value)) {
		return false
	}

	return true
}

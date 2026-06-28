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
	dst := make([]byte, 0, len(key)+1+len(value))
	dst = append(dst, key...)
	dst = append(dst, 0x00)

	return append(dst, value...)
}

// buildColumnBloom builds the bloom for one bloom-bearing column over its per-record values.
//   - FullText: a token per lowercased word of each value.
//   - Equality: each value verbatim (exact-match pruning, e.g. trace-by-id).
//   - Attrs: per attribute (k,v) of each serialized blob, the equality token k‖v and a full-text
//     token k‖word per value word.
func buildColumnBloom(mode BloomMode, values *byteCol) []byte {
	var (
		tokens  [][]byte
		words   [][]byte
		scratch []byte
	)

	rows := values.rows()

	switch mode {
	case BloomFullText:
		for i := range rows {
			tokens = bloom.Tokenize(tokens, values.at(i))
		}
	case BloomEquality:
		for i := range rows {
			tokens = append(tokens, values.at(i)) // each value is its own token
		}
	case BloomAttrs:
		for i := range rows {
			a, _, err := signal.DecodeAttributes(values.at(i))
			if err != nil {
				continue
			}

			for i := range a {
				scratch = a[i].Value.AppendText(scratch[:0])
				tokens = append(tokens, attrToken(a[i].Key, scratch))

				words = bloom.Tokenize(words[:0], scratch)
				for _, w := range words {
					tokens = append(tokens, attrToken(a[i].Key, w))
				}
			}
		}
	case BloomNone:
		return nil
	}

	f := bloom.New(len(tokens), 0.01)
	for _, tk := range tokens {
		f.Add(tk)
	}

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

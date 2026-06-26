package logengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Per-part bloom sidecars, written alongside the part's columns: body holds the body's full-text
// tokens; attrs holds each record's per-record attributes as exact key=value tokens.
const (
	bloomBodyObject  = "bloom-body.bin"
	bloomAttrsObject = "bloom-attrs.bin"
)

func bloomKey(prefix string) string     { return prefix + "/" + bloomBodyObject }
func attrBloomKey(prefix string) string { return prefix + "/" + bloomAttrsObject }

// attrToken is the bloom token for an attribute key=value pair: key ‖ 0x00 ‖ value-text. The value
// text is the same projection [fetch.EqualMatcher] carries, so the build side (the stored value)
// and the query side (Equal.Value) produce identical tokens. A separator collision can only cause
// a false positive (an extra scan), never a false negative.
func attrToken(dst, key, valueText []byte) []byte {
	dst = append(dst, key...)
	dst = append(dst, 0x00)

	return append(dst, valueText...)
}

// equalToken is the query-side token for an equality condition.
func equalToken(eq fetch.EqualMatcher) []byte {
	return attrToken(make([]byte, 0, len(eq.Name)+len(eq.Value)+1), []byte(eq.Name), []byte(eq.Value))
}

// buildBodyBloom tokenizes every body and returns the encoded bloom of all body tokens, sized to
// the token count.
func buildBodyBloom(bodies [][]byte) []byte {
	var toks [][]byte
	for _, b := range bodies {
		toks = bloom.Tokenize(toks, b)
	}

	f := bloom.New(len(toks), 0.01)
	for _, tk := range toks {
		f.Add(tk)
	}

	return f.Encode(nil)
}

// writeBodyBloom writes the part's body bloom object. Best-effort durability: it shares the part
// prefix, so [Engine.Reset] and [deletePart] (which list the prefix) clean it up too.
func writeBodyBloom(ctx context.Context, b backend.Backend, prefix string, bodies [][]byte) error {
	if err := b.Write(ctx, bloomKey(prefix), buildBodyBloom(bodies)); err != nil {
		return errors.Wrap(err, "write body bloom")
	}

	return nil
}

// loadBodyBloom reads and decodes a part's body bloom, returning nil (no error) when the object is
// absent — a part written before blooms existed simply is not prunable and is always scanned.
func loadBodyBloom(ctx context.Context, b backend.Backend, prefix string) (*bloom.Filter, error) {
	data, err := b.Read(ctx, bloomKey(prefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, nil //nolint:nilnil // absent bloom ⇒ no filter, no error
		}

		return nil, errors.Wrap(err, "read body bloom")
	}

	f, _, err := bloom.Decode(data)
	if err != nil {
		return nil, errors.Wrap(err, "decode body bloom")
	}

	return f, nil
}

// bodyTokensPresent reports whether the part may contain a body holding every full-text token of
// every `body` condition. A part is prunable only when it has a bloom; without one (nil) the
// answer is conservatively true (scan it). A single token testing absent rules the whole part out.
func (p *part) bodyTokensPresent(conds []fetch.Condition) bool {
	if p.bodyBloom == nil {
		return true
	}

	for i := range conds {
		if conds[i].Column != colBody {
			continue // body bloom holds body tokens only
		}

		for _, tok := range conds[i].Tokens {
			if !p.bodyBloom.Test(tok) {
				return false
			}
		}
	}

	return true
}

// buildAttrBloom returns the encoded bloom of every record's per-record attributes. For each
// attribute (key, value) it adds two kinds of key-scoped token: the **equality** token
// `key‖value-text` (so an exact `key=value` condition can prune), and one **full-text** token
// `key‖word` per lowercased word of the value (so a `key contains word` condition can prune). All
// tokens are key-scoped, so a value's word only matches a query against that same key.
func buildAttrBloom(attrs [][]byte) []byte {
	var (
		tokens  [][]byte
		scratch []byte
		words   [][]byte
	)

	for _, blob := range attrs {
		a, _, err := signal.DecodeAttributes(blob)
		if err != nil {
			continue // we wrote these blobs; a bad one just contributes no tokens
		}

		for i := range a {
			scratch = a[i].Value.AppendText(scratch[:0])
			tokens = append(tokens, attrToken(nil, a[i].Key, scratch))

			words = bloom.Tokenize(words[:0], scratch)
			for _, w := range words {
				tokens = append(tokens, attrToken(nil, a[i].Key, w))
			}
		}
	}

	f := bloom.New(len(tokens), 0.01)
	for _, tk := range tokens {
		f.Add(tk)
	}

	return f.Encode(nil)
}

// writeAttrBloom writes the part's attribute bloom object alongside its columns.
func writeAttrBloom(ctx context.Context, b backend.Backend, prefix string, attrs [][]byte) error {
	if err := b.Write(ctx, attrBloomKey(prefix), buildAttrBloom(attrs)); err != nil {
		return errors.Wrap(err, "write attr bloom")
	}

	return nil
}

// loadAttrBloom reads and decodes a part's attribute bloom, returning nil (no error) when absent.
func loadAttrBloom(ctx context.Context, b backend.Backend, prefix string) (*bloom.Filter, error) {
	data, err := b.Read(ctx, attrBloomKey(prefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, nil //nolint:nilnil // absent bloom ⇒ no filter, no error
		}

		return nil, errors.Wrap(err, "read attr bloom")
	}

	f, _, err := bloom.Decode(data)
	if err != nil {
		return nil, errors.Wrap(err, "decode attr bloom")
	}

	return f, nil
}

// attrConditionsPresent reports whether the part may hold a record satisfying every **attribute**
// condition's serializable hint: an `Equal` spec (exact key=value) and/or `Tokens` (a key-scoped
// `contains`, the non-equality path). Body conditions are handled by [part.bodyTokensPresent], so
// only conditions over a non-fixed column (an attribute key) are consulted here. Without an
// attribute bloom the answer is conservatively true; a single absent token rules the part out.
func (p *part) attrConditionsPresent(conds []fetch.Condition) bool {
	if p.attrBloom == nil {
		return true
	}

	for i := range conds {
		c := &conds[i]
		if isFixedColumn(c.Column) {
			continue // attribute (non-fixed-column) conditions only
		}

		if c.Equal != nil && !p.attrBloom.Test(equalToken(*c.Equal)) {
			return false
		}

		for _, tok := range c.Tokens {
			if !p.attrBloom.Test(attrToken(nil, []byte(c.Column), tok)) {
				return false
			}
		}
	}

	return true
}

// mayContain reports whether the part can be skipped for a conjunctive (AllConditions) query:
// false ⇒ a bloom proved a required token or equality value absent, so the part holds no matching
// record and is pruned. The engine still re-checks Match per row for surviving parts.
func (p *part) mayContain(conds []fetch.Condition) bool {
	return p.bodyTokensPresent(conds) && p.attrConditionsPresent(conds)
}

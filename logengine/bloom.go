package logengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
)

// bloomBodyObject is the per-part body-token bloom filter, written alongside the part's columns.
const bloomBodyObject = "bloom-body.bin"

func bloomKey(prefix string) string { return prefix + "/" + bloomBodyObject }

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

// bodyTokensPresent reports whether the part may contain a body holding every token of every
// full-text condition. A part is prunable only when it has a bloom; without one (nil), the answer
// is conservatively true (scan it). A single token testing absent rules the whole part out.
func (p *part) bodyTokensPresent(conds []fetch.Condition) bool {
	if p.bodyBloom == nil {
		return true
	}

	for i := range conds {
		for _, tok := range conds[i].Tokens {
			if !p.bodyBloom.Test(tok) {
				return false
			}
		}
	}

	return true
}

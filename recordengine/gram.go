package recordengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/internal/sparsegram"
	"github.com/oteldb/storage/query/fetch"
)

// A column's per-part gram filter is written to "{prefix}/grams-{column}.bin", beside — never
// instead of — the token bloom at "bloom-{column}.bin". The two filters hold different things and a
// gram probe against a token bloom would report absent for a gram the column really contains, which
// would prune a matching part. Separate keys make that confusion unrepresentable, and let a column
// carry both filters (or gain a gram index later without rewriting its parts).
func gramKey(prefix, column string) string { return prefix + "/grams-" + column + ".bin" }

// Gram-index parameters. These are part of the on-disk format: the gram set is a function of the
// bounds and of sparsegram's bigram weighting, so a build and a probe that disagree on them agree on
// nothing. They are stamped into each sidecar ([gramFormatVersion]) and a sidecar that does not
// match the running build is ignored rather than misread.
//
// 4/16 is measured, not guessed: against 3/24 it is 25% fewer bytes at indistinguishable
// selectivity, while 6/16 saves a further 15% but gives back most of the pruning. The minimum also
// sets the shortest prunable literal — a 3-byte substring hint yields no grams and prunes nothing.
const (
	gramMinLen = 4
	gramMaxLen = 16

	// gramFalsePositiveRate matches the full-text bloom's. A substring query probes several grams
	// and every one must test present for the part to survive, so the effective per-part rate is
	// p^(grams), far below p on any literal long enough to matter.
	gramFalsePositiveRate = 1e-2
)

// gramFormatVersion tags the sidecar. The bounds follow it so a retune is detected rather than
// silently misread: [decodeGramFilter] rejects a sidecar built with different ones.
const gramFormatVersion byte = 1

// gramScratch is the reusable state a gram build needs: the extractor (which keeps its bigram-weight
// and hull buffers) and the emitted ranges. It lives on [bloomBuilder] so a part's whole gram set is
// built out of one set of buffers, exactly like the token scratch beside it.
type gramScratch struct {
	ext   sparsegram.Extractor
	grams []sparsegram.Gram
}

// forEachGram calls fn once per gram of every value the builder selected. Grams alias the column's
// bytes — no gram is materialized — so fn must not retain them.
//
// It walks the same first-occurrence row set the token build uses ([bloomBuilder.selectRows]): a
// bloom is a set, and a repeated body re-derives grams the filter already holds. Gram extraction is
// the expensive part of this build (~4.6× a token build per byte), so skipping repeats matters more
// here than it does there.
func (bb *bloomBuilder) forEachGram(values *byteCol, fn func(gram []byte)) {
	bb.gram.ext.MinLen, bb.gram.ext.MaxLen = gramMinLen, gramMaxLen

	bb.each(values, func(i int) {
		v := values.at(i)

		bb.gram.grams = bb.gram.ext.Grams(bb.gram.grams[:0], v)
		for _, g := range bb.gram.grams {
			fn(v[g.Start:g.End])
		}
	})
}

// distinctGrams estimates the DISTINCT gram count [bloom.New] must be sized by. Sizing by
// occurrences is not an option here the way it briefly is for tokens: a log body yields hundreds of
// grams and they repeat heavily across rows, so an occurrence-sized filter would be oversized by the
// same order the token blooms once were.
func (bb *bloomBuilder) distinctGrams(values *byteCol) int {
	bb.distinct.Reset()
	bb.forEachGram(values, bb.distinct.Add)

	return bb.distinct.Estimate()
}

// buildGrams builds the gram sidecar for one column over its per-record values, or nil when the
// column holds nothing to index. Like [bloomBuilder.build] it walks the column twice — once to size,
// once to fill — rather than materializing every gram to learn the count.
func (bb *bloomBuilder) buildGrams(values *byteCol) []byte {
	if values.rows() == 0 {
		return nil
	}

	bb.markRows(values)

	f := bloom.New(bb.distinctGrams(values), gramFalsePositiveRate)
	bb.forEachGram(values, f.Add)

	return encodeGramFilter(f)
}

// buildColumnGrams is [bloomBuilder.buildGrams] on a throwaway builder, for callers with a single
// column to build and no builder to re-arm.
func buildColumnGrams(values *byteCol) []byte {
	var bb bloomBuilder

	return bb.buildGrams(values)
}

// encodeGramFilter wraps an encoded filter in the sidecar header: version, then the bounds the grams
// were extracted with.
func encodeGramFilter(f *bloom.Filter) []byte {
	dst := []byte{gramFormatVersion, gramMinLen, gramMaxLen}

	return f.Encode(dst)
}

// errGramFormat reports a sidecar this build must not read: a future version, or one whose grams
// were extracted with different bounds. Its gram set is a different set, so probing it would prune
// parts that match — the caller treats it as absent and scans, which is always safe.
var errGramFormat = errors.New("gram sidecar: format mismatch")

// decodeGramFilter parses a gram sidecar, or returns an [errGramFormat]-wrapping error for one this
// build must not read.
func decodeGramFilter(data []byte) (*bloom.Filter, error) {
	if len(data) < 3 {
		return nil, errors.New("gram sidecar: truncated header")
	}

	if data[0] != gramFormatVersion || data[1] != gramMinLen || data[2] != gramMaxLen {
		return nil, errors.Wrapf(errGramFormat, "version %d bounds %d/%d", data[0], data[1], data[2])
	}

	f, _, err := bloom.Decode(data[3:])
	if err != nil {
		return nil, err
	}

	return f, nil
}

// writeGrams writes the gram sidecar of each gram-bearing column of the schema. It shares the part
// prefix with the blooms, so deletePart / Reset clean it up.
func writeGrams(
	ctx context.Context, b backend.Backend, schema *Schema, prefix string, cols *recordCols, bb *bloomBuilder,
) error {
	for k := range schema.byteCols {
		col := schema.byteColumn(k)
		if !col.Grams {
			continue
		}

		data := bb.buildGrams(&cols.bytes[k])
		if data == nil {
			continue
		}

		if err := b.Write(ctx, gramKey(prefix, col.Name), data); err != nil {
			return errors.Wrapf(err, "write grams %q", col.Name)
		}
	}

	return nil
}

// gramHints holds the grams to probe per condition, extracted once per fetch instead of once per
// part: the extraction is cheap but a store with thousands of parts would repeat it that many times,
// and the result is identical for every part.
//
// Entry i belongs to conds[i]; a nil entry means that condition carries no substring hint. Grams
// alias the caller's literals, which outlive the fetch.
type gramHints [][][]byte

// buildGramHints extracts the covering grams of every condition's substring hints, or returns nil
// when no condition carries one — the common case, which then costs a single length check per part.
//
// Only the covering grams are kept ([sparsegram.Covering]): a value whose gram set holds a long gram
// holds the shorter grams inside it too, so probing those as well costs time and prunes nothing.
func buildGramHints(conds []fetch.Condition) gramHints {
	wanted := false

	for i := range conds {
		if len(conds[i].Substrings) > 0 {
			wanted = true

			break
		}
	}

	if !wanted {
		return nil
	}

	var (
		ext   sparsegram.Extractor
		buf   []sparsegram.Gram
		hints = make(gramHints, len(conds))
	)

	ext.MinLen, ext.MaxLen = gramMinLen, gramMaxLen

	for i := range conds {
		for _, lit := range conds[i].Substrings {
			buf = sparsegram.Covering(ext.Grams(buf[:0], lit))
			for _, g := range buf {
				hints[i] = append(hints[i], lit[g.Start:g.End])
			}
		}
	}

	return hints
}

// gramsMayMatch reports whether the part can hold a value containing every condition's substring
// hint — false ⇒ a required gram is provably absent from the column, so no value in the part
// contains the literal and the part need not be read.
//
// It demand-loads each probed column's filter through the engine's bounded cache, so it does I/O:
// call it from the lock-free part-read phase, never from the locked plan phase. A column with no
// gram sidecar, and a hint that yielded no grams (a literal shorter than gramMinLen), never prune.
//
// A read error is not fatal: the filter is only an optimization, so the part is scanned. Returning
// the error instead would fail a query that a slow or partially-unavailable backend could still
// answer correctly from the part itself.
func (p *part) gramsMayMatch(
	ctx context.Context, b backend.Backend, cache *gramCache, conds []fetch.Condition, hints gramHints,
) bool {
	for i := range conds {
		grams := hints[i]
		if len(grams) == 0 {
			continue
		}

		if !p.schema.hasGrams(conds[i].Column) {
			continue
		}

		f, err := gramFilterFor(ctx, cache, b, gramKey(p.prefix, conds[i].Column))
		if err != nil || f == nil {
			continue
		}

		for _, g := range grams {
			if !f.Test(g) {
				return false
			}
		}
	}

	return true
}

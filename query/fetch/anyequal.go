package fetch

import (
	"bytes"
	"slices"

	"github.com/oteldb/storage/signal"
)

// A set-membership predicate ("column is any of these N values") is the shape every id-resolving
// query lands on: TraceQL resolves candidate trace ids, then fetches the spans of exactly those
// traces. Expressed as a bare [Condition.Match] closure it reaches none of the engine's exact-match
// machinery — no part is bloom-pruned and every row pays a callback. [Condition.AnyEqual] carries
// the set itself, so the disjunction survives down to the bloom and to the per-part dictionary.
//
// The set is a **hint**, exactly as [Condition.Equal] is: it must be a superset of what Match
// accepts on that column (a row Match accepts has its column value in the set), so the engine may
// reject a non-member without calling Match but still verifies a member through Match. An embedder
// that sets neither is unaffected.

// AnyEqualSet normalizes vals into the shape [Condition.AnyEqual] requires — a fresh slice, sorted
// ascending and deduplicated — so a caller can hand over whatever order its candidate set came in.
// It returns nil for an empty input, which reads as "no hint" (see [Condition.AnyEqual]).
func AnyEqualSet(vals [][]byte) [][]byte {
	if len(vals) == 0 {
		return nil
	}

	out := slices.Clone(vals)
	slices.SortFunc(out, bytes.Compare)

	return slices.CompactFunc(out, bytes.Equal)
}

// AnyEqualStrings is [AnyEqualSet] over string members.
func AnyEqualStrings(vals []string) [][]byte {
	if len(vals) == 0 {
		return nil
	}

	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}

	return AnyEqualSet(out)
}

// InAnyEqual reports whether v is a member of set, which must be normalized ([AnyEqualSet]).
//
// The binary search is written out rather than taken from [slices.BinarySearchFunc]: this runs once
// per scanned row, and passing bytes.Compare as a function value costs an indirect call per probe
// where calling it directly compiles to the intrinsic.
func InAnyEqual(set [][]byte, v []byte) bool {
	lo, hi := 0, len(set)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(set[mid], v) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo < len(set) && bytes.Equal(set[lo], v)
}

// AnyEqualPredicate is the [Condition.Match] closure equivalent to membership in set — the
// predicate a peer reconstructs for a set pushed across the wire, mirroring
// [EqualMatcher.Predicate]. It compares the value's canonical text projection, which for a byte
// column is the cell's raw bytes.
func AnyEqualPredicate(set [][]byte) func(signal.Value) bool {
	return func(v signal.Value) bool {
		if v.Kind() == signal.KindStr {
			return InAnyEqual(set, v.Str())
		}

		return InAnyEqual(set, v.AppendText(nil))
	}
}

// SortedAnyEqual reports whether set is already in the normalized shape: strictly ascending, hence
// sorted and duplicate-free.
func SortedAnyEqual(set [][]byte) bool {
	for i := 1; i < len(set); i++ {
		if bytes.Compare(set[i-1], set[i]) >= 0 {
			return false
		}
	}

	return true
}

// NormalizeConditions returns conds with every [Condition.AnyEqual] in the normalized shape
// [InAnyEqual] needs. It returns conds itself when every set is already normalized (the common
// case, and the only cost an embedder that sets no set at all pays: one length check per
// condition); otherwise it returns a shallow clone with the offending sets replaced, leaving the
// caller's slices untouched.
//
// A producer calls this once per request rather than trusting the field's documented shape, so an
// unsorted set from an embedder costs a sort instead of silently dropping rows.
func NormalizeConditions(conds []Condition) []Condition {
	dirty := -1

	for i := range conds {
		if len(conds[i].AnyEqual) > 1 && !SortedAnyEqual(conds[i].AnyEqual) {
			dirty = i

			break
		}
	}

	if dirty < 0 {
		return conds
	}

	out := slices.Clone(conds)
	for i := dirty; i < len(out); i++ {
		if len(out[i].AnyEqual) > 1 && !SortedAnyEqual(out[i].AnyEqual) {
			out[i].AnyEqual = AnyEqualSet(out[i].AnyEqual)
		}
	}

	return out
}

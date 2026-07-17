package bloom

// SafeTokens appends to dst the tokens that are provably present in *any* value a `contains lit`
// predicate matches, and returns the extended slice. It is the query-side companion to [Tokenize]:
// an embedder that lowers a substring/regexp filter to a required literal feeds that literal here,
// sets the result as a fetch condition's token hint, and the per-part token bloom then prunes any
// part whose bloom lacks one of these tokens — without ever pruning a part that holds a real match.
//
// The safety rule. The bloom holds whole tokens (maximal alphanumeric runs). A value containing lit
// as a substring may glue extra alphanumerics onto lit's first/last token — "GET" occurs inside
// "xGETy", whose token is "xgety", not "get" — so lit's edge tokens are NOT guaranteed to be whole
// tokens of the value, and testing them would wrongly prune a match. SafeTokens therefore drops the
// leading and trailing partial token (the run of alphanumerics touching each edge) before
// tokenizing, keeping only interior tokens that a separator bounds on both sides within lit. Every
// returned token T satisfies: a value that contains lit contains T as a whole token, i.e.
// T ∈ Tokenize(value). The extraction under-approximates by design — a single-word literal yields
// no tokens (no pruning, a full scan, still correct) rather than an unsafe one.
//
// leftPinned/rightPinned tell SafeTokens an edge cannot be extended, so its edge token is safe to
// keep: set them when an anchor (^, $), a word boundary (\b), or an exact-equality match guarantees
// the value does not glue an alphanumeric onto that side of lit. With both pinned lit is matched
// exactly, so every token of lit is safe.
func SafeTokens(dst [][]byte, lit []byte, leftPinned, rightPinned bool) [][]byte {
	lo, hi := 0, len(lit)

	if !leftPinned {
		for lo < hi && isAlnum(lit[lo]) {
			lo++
		}
	}

	if !rightPinned {
		for hi > lo && isAlnum(lit[hi-1]) {
			hi--
		}
	}

	return Tokenize(dst, lit[lo:hi])
}

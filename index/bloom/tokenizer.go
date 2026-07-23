package bloom

// Tokenize splits s into lowercased tokens — maximal runs of ASCII letters/digits — appending each
// to dst and returning the extended slice. Non-alphanumeric bytes are separators. Each token is a
// freshly allocated, lowercased copy (it does not alias s). It is the tokenization used both to
// fill a body bloom at flush and to derive a query's required tokens, so the two agree.
//
// It allocates a token at a time, which suits the query side (a handful of tokens per predicate).
// A flush tokenizing whole columns should use [Scanner] instead, which yields the same tokens
// without allocating.
func Tokenize(dst [][]byte, s []byte) [][]byte {
	var sc Scanner

	sc.Reset(s)
	for {
		tok, ok := sc.Next()
		if !ok {
			return dst
		}

		dst = append(dst, append([]byte(nil), tok...))
	}
}

// CountTokens returns how many tokens s yields, without producing any. Sizing a filter from it
// and then filling the filter from a [Scanner] over the same input costs no token materialization.
func CountTokens(s []byte) int {
	n := 0
	for i := 0; i < len(s); {
		if alnumLower[s[i]] == 0 {
			i++

			continue
		}

		for i < len(s) && alnumLower[s[i]] != 0 {
			i++
		}

		n++
	}

	return n
}

// Scanner walks the lowercased tokens of a byte slice — the same tokens [Tokenize] produces —
// without allocating per token. A token that is already lowercase aliases the input; one holding
// uppercase is folded into the scanner's reusable buffer. Either way the returned slice is valid
// only until the next call to [Scanner.Next] or [Scanner.Reset], so a caller that retains a token
// must copy it.
//
// The zero value is ready to use. Reuse one scanner across values so the fold buffer is kept. Not
// safe for concurrent use.
type Scanner struct {
	src []byte
	pos int
	buf []byte
}

// Reset points the scanner at s, rewinding it.
func (sc *Scanner) Reset(s []byte) {
	sc.src, sc.pos = s, 0
}

// Next returns the next token and whether one was found.
func (sc *Scanner) Next() ([]byte, bool) {
	i := sc.pos
	for i < len(sc.src) && alnumLower[sc.src[i]] == 0 {
		i++
	}

	if i == len(sc.src) {
		sc.pos = i

		return nil, false
	}

	j := i
	for j < len(sc.src) && alnumLower[sc.src[j]] != 0 {
		j++
	}

	sc.pos = j

	return sc.fold(sc.src[i:j]), true
}

// fold returns tok ASCII-lowercased: tok itself when it is already lowercase (the common case, so
// no copy), else a lowercased copy in the reusable buffer. A byte needs folding exactly when the
// table maps it to something other than itself.
func (sc *Scanner) fold(tok []byte) []byte {
	upper := -1
	for i, c := range tok {
		if alnumLower[c] != c {
			upper = i

			break
		}
	}

	if upper < 0 {
		return tok
	}

	sc.buf = append(sc.buf[:0], tok...)
	for i := upper; i < len(sc.buf); i++ {
		sc.buf[i] = alnumLower[sc.buf[i]]
	}

	return sc.buf
}

// alnumLower classifies and case-folds in a single table load: a letter or digit maps to its ASCII
// lowercase form (never 0), every other byte — a separator — maps to 0. It replaces the three range
// comparisons a classification test would branch through, and lets the fold reuse the same lookup
// (a byte needs folding exactly when alnumLower[c] != c). Indexing a [256]byte by a byte is
// provably in range, so no bounds check is emitted.
var alnumLower = buildAlnumLower()

func buildAlnumLower() (t [256]byte) {
	for i := range 256 {
		switch c := byte(i); {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			t[i] = c
		case c >= 'A' && c <= 'Z':
			t[i] = c + 'a' - 'A'
		}
	}

	return t
}

func isAlnum(c byte) bool { return alnumLower[c] != 0 }

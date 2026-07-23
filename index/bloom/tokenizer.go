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
		if !isAlnum(s[i]) {
			i++

			continue
		}

		for i < len(s) && isAlnum(s[i]) {
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
	for i < len(sc.src) && !isAlnum(sc.src[i]) {
		i++
	}

	if i == len(sc.src) {
		sc.pos = i

		return nil, false
	}

	j := i
	for j < len(sc.src) && isAlnum(sc.src[j]) {
		j++
	}

	sc.pos = j

	return sc.fold(sc.src[i:j]), true
}

// fold returns tok ASCII-lowercased: tok itself when it holds no uppercase (the common case, so no
// copy), else a lowercased copy in the reusable buffer.
func (sc *Scanner) fold(tok []byte) []byte {
	upper := -1
	for i, c := range tok {
		if c >= 'A' && c <= 'Z' {
			upper = i

			break
		}
	}

	if upper < 0 {
		return tok
	}

	sc.buf = append(sc.buf[:0], tok...)
	for i := upper; i < len(sc.buf); i++ {
		if c := sc.buf[i]; c >= 'A' && c <= 'Z' {
			sc.buf[i] = c + 'a' - 'A'
		}
	}

	return sc.buf
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

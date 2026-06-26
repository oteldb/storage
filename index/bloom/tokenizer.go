package bloom

// Tokenize splits s into lowercased tokens — maximal runs of ASCII letters/digits — appending each
// to dst and returning the extended slice. Non-alphanumeric bytes are separators. Each token is a
// freshly allocated, lowercased copy (it does not alias s). It is the tokenization used both to
// fill a body bloom at flush and to derive a query's required tokens, so the two agree.
func Tokenize(dst [][]byte, s []byte) [][]byte {
	start := -1

	for i := 0; i <= len(s); i++ {
		alnum := i < len(s) && isAlnum(s[i])
		if alnum {
			if start < 0 {
				start = i
			}

			continue
		}

		if start >= 0 {
			dst = append(dst, lowerClone(s[start:i]))
			start = -1
		}
	}

	return dst
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// lowerClone returns a lowercased copy of s (ASCII).
func lowerClone(s []byte) []byte {
	out := make([]byte, len(s))
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}

		out[i] = c
	}

	return out
}

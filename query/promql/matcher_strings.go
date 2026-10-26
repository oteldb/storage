package promql

import (
	"regexp/syntax"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/fetch"
)

func matcherStrings(m *labels.Matcher) [][]byte {
	if m.Type == labels.MatchEqual && m.Value == "" {
		return [][]byte{{}}
	}
	if m.Type != labels.MatchRegexp {
		return nil
	}
	values := m.SetMatches()
	if len(values) == 0 {
		return nil
	}
	re, err := syntax.Parse(m.Value, syntax.Perl|syntax.DotNL)
	if err != nil || !completeFiniteRegexp(re, 0) {
		return nil
	}
	bytes := 0
	for _, v := range values {
		bytes += len(v)
		if bytes > 4096 {
			return nil
		}
	}
	return fetch.AnyEqualStrings(values)
}

func completeFiniteRegexp(re *syntax.Regexp, depth int) bool {
	if depth > 64 || re.Flags&syntax.FoldCase != 0 {
		return false
	}
	switch re.Op {
	case syntax.OpLiteral:
		for _, r := range re.Rune {
			if !utf8.ValidRune(r) || r == utf8.RuneError {
				return false
			}
		}
		return true
	case syntax.OpCharClass:
		for i := 0; i < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			if lo <= utf8.RuneError && utf8.RuneError <= hi || lo <= 0xdfff && hi >= 0xd800 {
				return false
			}
		}
		return true
	case syntax.OpBeginText, syntax.OpEndText:
		return true
	case syntax.OpCapture, syntax.OpConcat, syntax.OpAlternate:
		for _, sub := range re.Sub {
			if !completeFiniteRegexp(sub, depth+1) {
				return false
			}
		}
		return true
	default:
		// Empty branches can be dropped by upstream finite extraction; keep the original matcher.
		return false
	}
}

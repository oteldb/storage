package fetch

import "github.com/oteldb/storage/signal"

// StringSetMatcher is a serializable necessary hint for string-valued identity labels.
// Other value kinds, including absence, require the requester's original callback.
type StringSetMatcher struct {
	Name   string
	Values [][]byte
}

// Matcher reconstructs a conservative predicate, normalizing Values without changing the input.
func (m StringSetMatcher) Matcher() Matcher {
	values := m.Values
	if !SortedAnyEqual(values) {
		values = AnyEqualSet(values)
	}
	return Matcher{
		Name: []byte(m.Name), StringValues: values,
		Match: func(v signal.Value) bool {
			return len(values) == 0 || v.Kind() != signal.KindStr || InAnyEqual(values, v.Str())
		},
	}
}

// StringSpecs extracts finite string hints; their predicates remain supersets of the callbacks.
func StringSpecs(matchers []Matcher) []StringSetMatcher {
	var out []StringSetMatcher
	for _, m := range matchers {
		if len(m.StringValues) > 0 {
			out = append(out, StringSetMatcher{Name: string(m.Name), Values: m.StringValues})
		}
	}
	return out
}

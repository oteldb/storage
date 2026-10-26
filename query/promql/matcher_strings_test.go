package promql

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMatcherStringHints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		typ     labels.MatchType
		pattern string
		values  []string
	}{
		{"alternation", labels.MatchRegexp, "foo|bar", []string{"bar", "foo"}},
		{"literal", labels.MatchRegexp, "foo", []string{"foo"}},
		{"concat", labels.MatchRegexp, "a(b|c)", []string{"ab", "ac"}},
		{"class", labels.MatchRegexp, "a[bc]", []string{"ab", "ac"}},
		{"anchors", labels.MatchRegexp, "^(foo|bar)$", []string{"bar", "foo"}},
		{"empty equality", labels.MatchEqual, "", []string{""}},
		{"nonempty equality", labels.MatchEqual, "foo", nil},
		{"negative equality", labels.MatchNotEqual, "foo", nil},
		{"negative regex", labels.MatchNotRegexp, "foo|bar", nil},
		{"empty branch", labels.MatchRegexp, "(|foo)", nil},
		{"empty branch prefix", labels.MatchRegexp, "(|foo)bar", nil},
		{"folding", labels.MatchRegexp, "(?i)foo", nil},
		{"repetition", labels.MatchRegexp, "foo.*", nil},
		{"replacement rune", labels.MatchRegexp, "\ufffd|foo", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := labels.MustNewMatcher(tt.typ, "env", tt.pattern)
			pushed := PushableMatchers([]*labels.Matcher{m})
			require.Len(t, pushed, 1)
			assert.Equal(t, fetch.AnyEqualStrings(tt.values), pushed[0].StringValues)
			for _, value := range []signal.Value{signal.EmptyValue(), signal.StringValue(nil), signal.StringValue([]byte("foo")), signal.IntValue(7)} {
				assert.Equal(t, m.Matches(string(value.AppendText(nil))), pushed[0].Match(value))
			}
			if len(pushed[0].StringValues) > 0 {
				hint := (fetch.StringSetMatcher{Values: pushed[0].StringValues}).Matcher()
				assert.True(t, hint.Match(signal.EmptyValue()))
				assert.True(t, hint.Match(signal.IntValue(7)))
			}
		})
	}
}

func FuzzMatcherStringHints(f *testing.F) {
	for _, seed := range [][2]string{
		{"foo|bar", "foo"}, {"a(b|c)", "ab"}, {"(|foo)bar", "bar"}, {"foo|bar", "xfooy"},
		{"\ufffd|foo", "\xff"}, {"[a-z]", "x"}, {"(?i)foo", "FOO"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		m, err := labels.NewMatcher(labels.MatchRegexp, "label", pattern)
		if err != nil {
			return
		}
		values := matcherStrings(m)
		if len(values) > 0 && m.Matches(value) && !fetch.InAnyEqual(values, []byte(value)) {
			t.Fatalf("string hint drops accepted value %q for %q", value, pattern)
		}
	})
}

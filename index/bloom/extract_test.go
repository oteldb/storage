package bloom

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toStrings(toks [][]byte) []string {
	if len(toks) == 0 {
		return nil
	}

	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = string(t)
	}

	return out
}

func TestSafeTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		lit                     string
		leftPinned, rightPinned bool
		want                    []string
	}{
		{name: "empty"},
		{name: "single word drops to nothing", lit: "GET"},
		{name: "two words both edges unsafe", lit: "foo bar"},
		{name: "three words keeps interior", lit: "foo bar baz", want: []string{"bar"}},
		{name: "many interior words", lit: "a bb ccc dd e", want: []string{"bb", "ccc", "dd"}},
		{name: "dot separators", lit: "a.b.c", want: []string{"b"}},
		{name: "leading separator makes first word safe", lit: " foo bar", want: []string{"foo"}},
		{name: "trailing separator makes last word safe", lit: "foo bar ", want: []string{"bar"}},
		{name: "both separators keep both", lit: " foo bar ", want: []string{"foo", "bar"}},
		{name: "lowercased", lit: "x ERROR y", want: []string{"error"}},
		{name: "left pinned keeps first only if right-bounded", lit: "error foo", leftPinned: true, want: []string{"error"}},
		{name: "left pinned single word still unsafe on right", lit: "error", leftPinned: true},
		{name: "right pinned keeps last", lit: "foo bar", rightPinned: true, want: []string{"bar"}},
		{name: "exact match keeps every token", lit: "foo bar baz", leftPinned: true, rightPinned: true, want: []string{"foo", "bar", "baz"}},
		{name: "exact single word", lit: "error", leftPinned: true, rightPinned: true, want: []string{"error"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SafeTokens(nil, []byte(tt.lit), tt.leftPinned, tt.rightPinned)
			assert.Equal(t, tt.want, toStrings(got))
		})
	}
}

// FuzzSafeTokens asserts the necessary-token invariant that makes bloom pruning safe: for an
// unpinned substring literal, every extracted token must be a whole token of any value that
// contains the literal — otherwise the bloom would prune a genuine match. It builds such a value
// as prefix+lit+suffix (an arbitrary occurrence) and checks each token is in its tokenization.
func FuzzSafeTokens(f *testing.F) {
	f.Add([]byte("foo bar baz"), []byte("pre"), []byte("suf"))
	f.Add([]byte("GET"), []byte("x"), []byte("y"))
	f.Add([]byte("a.b.c"), []byte(""), []byte(""))
	f.Add([]byte(" foo bar "), []byte("x"), []byte("y"))

	f.Fuzz(func(t *testing.T, lit, prefix, suffix []byte) {
		value := make([]byte, 0, len(prefix)+len(lit)+len(suffix))
		value = append(value, prefix...)
		value = append(value, lit...)
		value = append(value, suffix...)

		valueTokens := make(map[string]struct{})
		for _, tok := range Tokenize(nil, value) {
			valueTokens[string(tok)] = struct{}{}
		}

		for _, tok := range SafeTokens(nil, lit, false, false) {
			_, ok := valueTokens[string(tok)]
			require.Truef(t, ok, "extracted token %q absent from Tokenize(%q) — would falsely prune", tok, value)
		}
	})
}

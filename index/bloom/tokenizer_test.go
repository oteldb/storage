package bloom

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scanAll drains a [Scanner] over s, copying each token (they alias the scanner's scratch).
func scanAll(sc *Scanner, s []byte) [][]byte {
	var out [][]byte

	sc.Reset(s)
	for {
		tok, ok := sc.Next()
		if !ok {
			return out
		}

		out = append(out, append([]byte(nil), tok...))
	}
}

func TestScannerMatchesTokenize(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", []string{}},
		{"single", "hello", []string{"hello"}},
		{"separators", "a-b_c d", []string{"a", "b", "c", "d"}},
		{"leading and trailing separators", "--ab--", []string{"ab"}},
		{"no alphanumerics", "--- ///", []string{}},
		{"mixed case", "MiXeD CaSe", []string{"mixed", "case"}},
		{"all upper", "ABC", []string{"abc"}},
		{"digits", "v1 2026 x9", []string{"v1", "2026", "x9"}},
		{"unicode is a separator", "héllo", []string{"h", "llo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sc Scanner
			got := scanAll(&sc, []byte(tt.in))

			gotStr := make([]string, 0, len(got))
			for _, tok := range got {
				gotStr = append(gotStr, string(tok))
			}

			assert.Equal(t, tt.want, gotStr)
			assert.Equal(t, len(tt.want), CountTokens([]byte(tt.in)))
			assert.Equal(t, Tokenize(nil, []byte(tt.in)), got)
		})
	}
}

// TestScannerReuseAcrossValues checks that the case-folding buffer, retained across values, does
// not leak the previous token into the next one — the failure mode a reusable buffer invites.
func TestScannerReuseAcrossValues(t *testing.T) {
	t.Parallel()

	var sc Scanner

	inputs := []string{"LONGUPPERCASETOKEN", "ab", "Cd", "x", "MiXeD"}
	want := [][]string{{"longuppercasetoken"}, {"ab"}, {"cd"}, {"x"}, {"mixed"}}

	for i, in := range inputs {
		got := scanAll(&sc, []byte(in))
		require.Len(t, got, len(want[i]))

		for j, tok := range got {
			require.Equal(t, want[i][j], string(tok))
		}
	}
}

// TestScannerTokenValidUntilNext documents the aliasing contract: a token is only valid until the
// next call, so a caller that keeps one must copy it.
func TestScannerTokenValidUntilNext(t *testing.T) {
	t.Parallel()

	var sc Scanner

	sc.Reset([]byte("Alpha Beta"))
	first, ok := sc.Next()
	require.True(t, ok)
	require.Equal(t, "alpha", string(first))

	second, ok := sc.Next()
	require.True(t, ok)
	require.Equal(t, "beta", string(second))
}

// FuzzScannerMatchesTokenize pins the allocation-free scanner to [Tokenize], the tokenization the
// query side uses. A divergence would make a flush index tokens a query never asks for (or the
// reverse), which shows up as wrongly pruned parts rather than an error.
func FuzzScannerMatchesTokenize(f *testing.F) {
	f.Add("hello world")
	f.Add("MiXeD CaSe TOKENS")
	f.Add("")
	f.Add("--- ///")
	f.Add("héllo wörld")
	f.Add("UPPER-lower_123")

	f.Fuzz(func(t *testing.T, s string) {
		want := Tokenize(nil, []byte(s))

		var sc Scanner
		got := scanAll(&sc, []byte(s))

		if len(want) != len(got) {
			t.Fatalf("token count: Tokenize %d, Scanner %d", len(want), len(got))
		}

		if CountTokens([]byte(s)) != len(want) {
			t.Fatalf("CountTokens %d, Tokenize %d", CountTokens([]byte(s)), len(want))
		}

		for i := range want {
			if !bytes.Equal(want[i], got[i]) {
				t.Fatalf("token %d: Tokenize %q, Scanner %q", i, want[i], got[i])
			}
		}
	})
}

func BenchmarkScanner(b *testing.B) {
	line := []byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user_id=8f3a2b91 " +
		"latency_ms=42 status=OK region=eu-central-1")

	var sc Scanner

	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()

	for b.Loop() {
		sc.Reset(line)
		for {
			if _, ok := sc.Next(); !ok {
				break
			}
		}
	}
}

func BenchmarkTokenize(b *testing.B) {
	line := []byte("2026-07-23T09:45:37Z INFO checkout-service handler=CreateOrder user_id=8f3a2b91 " +
		"latency_ms=42 status=OK region=eu-central-1")

	var dst [][]byte

	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()

	for b.Loop() {
		dst = Tokenize(dst[:0], line)
	}

	_ = dst
}

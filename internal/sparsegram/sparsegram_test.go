package sparsegram_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/sparsegram"
)

func gramStrings(tb testing.TB, e *sparsegram.Extractor, s string) []string {
	tb.Helper()

	b := []byte(s)
	grams := e.Grams(nil, b)
	out := make([]string, 0, len(grams))

	for _, g := range grams {
		out = append(out, string(b[g.Start:g.End]))
	}

	return out
}

func TestGramsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		minLen int
		maxLen int
	}{
		{"empty", "", 0, 0},
		{"below min", "ab", 0, 0},
		{"exactly min", "abc", 0, 0},
		{"log line", "trace[1242817017] linearizableReadLoop", 0, 0},
		{"hex id", "4bf92f3577b34da6a3ce929d0e0e4736", 0, 0},
		{"tight max", "the quick brown fox jumps", 3, 6},
		{"wide", "the quick brown fox jumps", 4, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &sparsegram.Extractor{MinLen: tt.minLen, MaxLen: tt.maxLen}
			lo, hi := tt.minLen, tt.maxLen

			if lo <= 0 {
				lo = sparsegram.DefaultMinLen
			}

			if hi <= 0 {
				hi = sparsegram.DefaultMaxLen
			}

			for _, g := range gramStrings(t, e, tt.in) {
				assert.GreaterOrEqual(t, len(g), lo, "gram %q under MinLen", g)
				assert.LessOrEqual(t, len(g), hi, "gram %q over MaxLen", g)
				assert.Contains(t, tt.in, g, "gram %q is not a substring of the input", g)
			}
		})
	}
}

// TestGramsAreDeterministic pins that the gram set depends on the bytes alone — the property that
// lets a build side and a query side agree without sharing state.
func TestGramsAreDeterministic(t *testing.T) {
	t.Parallel()

	var a, b sparsegram.Extractor

	const s = "level=error msg=\"connection refused\" peer=10.0.1.23:40698 attempt=7"

	// Walk unrelated values through b first: scratch reuse must not leak into the result.
	for _, warm := range []string{"", "x", "another value entirely", strings.Repeat("z", 300)} {
		b.Grams(nil, []byte(warm))
	}

	assert.Equal(t, gramStrings(t, &a, s), gramStrings(t, &b, s))
}

// TestNeedleGramsAreHaystackGrams is the safety property the whole design rests on: because a gram
// is chosen by bytes inside it alone, every gram of a needle is a gram of any value containing that
// needle. If this fails, probing a bloom with the needle's grams can prune a part that really
// matches.
func TestNeedleGramsAreHaystackGrams(t *testing.T) {
	t.Parallel()

	needles := []string{
		"deadbeef",
		"1242817017",
		"linearizableReadLoop",
		"billing-reconciler",
		"4bf92f3577b34da6a3ce929d0e0e4736",
	}

	prefixes := []string{"", "x", "prefix ", "trace[", "aaaaaaaaaaaaaaaa", "\x00\xff "}
	suffixes := []string{"", "y", " suffix", "] linearizableReadLoop", strings.Repeat("q", 40)}

	var ne, he sparsegram.Extractor

	for _, needle := range needles {
		for _, p := range prefixes {
			for _, s := range suffixes {
				hay := p + needle + s
				want := gramStrings(t, &ne, needle)
				got := gramStrings(t, &he, hay)

				for _, g := range want {
					assert.Contains(t, got, g,
						"gram %q of needle %q missing from haystack %q", g, needle, hay)
				}
			}
		}
	}
}

// TestSingleRunLiteralYieldsGrams is the issue this package exists for: a one-word literal that
// index/bloom.SafeTokens reduces to nothing still produces probes here.
func TestSingleRunLiteralYieldsGrams(t *testing.T) {
	t.Parallel()

	var e sparsegram.Extractor

	for _, lit := range []string{"deadbeef", "1242817017", "linearizableReadLoop"} {
		assert.NotEmpty(t, gramStrings(t, &e, lit), "literal %q produced no grams", lit)
	}
}

func TestCovering(t *testing.T) {
	t.Parallel()

	var e sparsegram.Extractor

	const s = "trace[1242817017] linearizableReadLoop"

	b := []byte(s)
	all := e.Grams(nil, b)
	cov := sparsegram.Covering(append([]sparsegram.Gram(nil), all...))

	require.NotEmpty(t, cov)
	assert.LessOrEqual(t, len(cov), len(all))

	// Every dropped gram must be contained in a kept one, or the query side would lose a probe it
	// could not recover.
	for _, g := range all {
		found := false

		for _, k := range cov {
			if k.Start <= g.Start && k.End >= g.End {
				found = true

				break
			}
		}

		assert.True(t, found, "gram %q dropped without a covering gram", s[g.Start:g.End])
	}
}

func TestAppendHashesMatchesGrams(t *testing.T) {
	t.Parallel()

	var e sparsegram.Extractor

	b := []byte("level=error msg=\"connection refused\" peer=10.0.1.23")

	grams := e.Grams(nil, b)
	want := make([]uint64, 0, len(grams))

	for _, g := range grams {
		want = append(want, sparsegram.HashGram(b[g.Start:g.End]))
	}

	assert.Equal(t, want, e.AppendHashes(nil, b))
}

// FuzzNeedleGramsAreHaystackGrams is the property test above, unbounded. A counter-example here is
// a correctness bug: it means a real match could be pruned away.
func FuzzNeedleGramsAreHaystackGrams(f *testing.F) {
	f.Add("deadbeef", "trace[", "] done")
	f.Add("1242817017", "", "")
	f.Add("abc", "a", "a")
	f.Add(strings.Repeat("ab", 32), "zz", "zz")

	f.Fuzz(func(t *testing.T, needle, prefix, suffix string) {
		if len(needle) > 1<<12 || len(prefix) > 1<<12 || len(suffix) > 1<<12 {
			t.Skip()
		}

		var ne, he sparsegram.Extractor

		nb := []byte(needle)
		hb := []byte(prefix + needle + suffix)

		hay := map[string]struct{}{}
		for _, g := range he.Grams(nil, hb) {
			hay[string(hb[g.Start:g.End])] = struct{}{}
		}

		for _, g := range ne.Grams(nil, nb) {
			tok := string(nb[g.Start:g.End])
			if _, ok := hay[tok]; !ok {
				t.Fatalf("gram %q of needle %q missing from haystack %q", tok, needle, string(hb))
			}
		}
	})
}

// FuzzGramsAreSubstrings guards the weaker invariant that every emitted range is in bounds.
func FuzzGramsAreSubstrings(f *testing.F) {
	f.Add("hello world")
	f.Add("")
	f.Add("\x00\x00\x00")

	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 1<<14 {
			t.Skip()
		}

		var e sparsegram.Extractor

		b := []byte(s)
		for _, g := range e.Grams(nil, b) {
			if g.Start < 0 || g.End > int32(len(b)) || g.Start >= g.End {
				t.Fatalf("out-of-range gram %+v for %q", g, s)
			}

			if !bytes.Contains(b, b[g.Start:g.End]) {
				t.Fatalf("gram %q not a substring", b[g.Start:g.End])
			}
		}
	})
}

func BenchmarkGrams(b *testing.B) {
	lines := [][]byte{
		[]byte("Fallback node addresses updated"),
		[]byte("trace[1242817017] linearizableReadLoop"),
		[]byte(`router: completed GET /api/healthz for 10.0.1.23:40698, 200 OK in 0.2ms`),
		[]byte("4bf92f3577b34da6a3ce929d0e0e4736 8ea75fea0695d78a"),
	}

	total := 0
	for _, l := range lines {
		total += len(l)
	}

	var e sparsegram.Extractor

	scratch := make([]sparsegram.Gram, 0, 512)

	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ResetTimer()

	for b.Loop() {
		for _, l := range lines {
			scratch = e.Grams(scratch[:0], l)
		}
	}

	b.ReportMetric(float64(len(scratch)), "grams/lastline")
}

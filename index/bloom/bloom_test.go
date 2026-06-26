package bloom

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoFalseNegatives(t *testing.T) {
	t.Parallel()

	f := New(100, 0.01)

	added := make([][]byte, 0, 100)
	for i := range 100 {
		tok := fmt.Appendf(nil, "token-%d", i)
		f.Add(tok)
		added = append(added, tok)
	}

	for _, tok := range added {
		assert.Truef(t, f.Test(tok), "an added token always tests present: %q", tok)
	}
}

func TestAbsentTokensMostlyRejected(t *testing.T) {
	t.Parallel()

	f := New(100, 0.01)
	for i := range 100 {
		f.Add(fmt.Appendf(nil, "present-%d", i))
	}

	falsePositives := 0
	const trials = 1000

	for i := range trials {
		if f.Test(fmt.Appendf(nil, "absent-%d", i)) {
			falsePositives++
		}
	}

	assert.Lessf(t, falsePositives, trials/10, "false-positive rate near the 1%% target, got %d/%d", falsePositives, trials)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	f := New(50, 0.02)
	for i := range 50 {
		f.Add(fmt.Appendf(nil, "t%d", i))
	}

	enc := f.Encode(nil)
	got, n, err := Decode(enc)
	require.NoError(t, err)
	assert.Equal(t, len(enc), n, "consumed the whole encoding")
	assert.Equal(t, f.k, got.k)
	assert.Equal(t, f.m, got.m)

	for i := range 50 {
		assert.True(t, got.Test(fmt.Appendf(nil, "t%d", i)), "decoded filter preserves membership")
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	t.Parallel()

	_, _, err := Decode(nil)
	require.Error(t, err)
	_, _, err = Decode([]byte{0x09})
	require.Error(t, err, "bad version")

	enc := New(8, 0.01).Encode(nil)
	enc[len(enc)-1] ^= 0xff // corrupt the CRC
	_, _, err = Decode(enc)
	require.Error(t, err, "CRC mismatch surfaced")
}

func TestTokenize(t *testing.T) {
	t.Parallel()

	got := Tokenize(nil, []byte("GET /api/v1/Users?id=42 OK"))
	want := [][]byte{[]byte("get"), []byte("api"), []byte("v1"), []byte("users"), []byte("id"), []byte("42"), []byte("ok")}
	assert.Equal(t, want, got, "lowercased alphanumeric tokens, separators dropped")

	assert.Empty(t, Tokenize(nil, []byte("   ---  ")), "no tokens in punctuation-only input")
}

// FuzzFilterRoundTrip asserts Decode never panics and that a freshly built+encoded filter
// round-trips with no false negatives for the fuzzed items.
func FuzzFilterRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte(""), []byte("x"))

	f.Fuzz(func(t *testing.T, a, b []byte) {
		flt := New(2, 0.01)
		flt.Add(a)
		flt.Add(b)
		require.True(t, flt.Test(a))
		require.True(t, flt.Test(b))

		got, _, err := Decode(flt.Encode(nil))
		require.NoError(t, err)
		require.True(t, got.Test(a))
		require.True(t, got.Test(b))
	})
}

// FuzzDecodeNeverPanics feeds arbitrary bytes to Decode.
func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _, _ = Decode(data)
	})
}

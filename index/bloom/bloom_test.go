package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

// FuzzDecodeNeverPanics feeds arbitrary bytes to Decode: it must not panic, and whatever it accepts
// must be a usable filter — a probe count outside [1, maxProbes] is either an always-true filter or
// unbounded per-condition CPU.
func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	f.Add(New(4, 0.01).Encode(nil))
	f.Add(encodeWithProbes(0, 64))
	f.Add(encodeWithProbes(1<<63, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, _, err := Decode(data)
		if err != nil {
			return
		}

		if got.k < 1 || got.k > maxProbes {
			t.Fatalf("decoded k = %d, outside [1, %d]", got.k, maxProbes)
		}

		got.Test([]byte("probe"))
	})
}

// encodeWithProbes hand-builds a CRC-valid encoding declaring an arbitrary k, which [Filter.Encode]
// cannot produce — the shape a corrupt or hostile sidecar takes.
func encodeWithProbes(k, m uint64) []byte {
	dst := []byte{encodeVersion}
	dst = binary.AppendUvarint(dst, k)
	dst = binary.AppendUvarint(dst, m)
	dst = append(dst, make([]byte, m/8)...)

	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst, castagnoli))
}

func TestDecodeRejectsProbeCount(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		k    uint64
		ok   bool
	}{
		{"zero always tests present", 0, false},
		{"one", 1, true},
		{"max", maxProbes, true},
		{"above max", maxProbes + 1, false},
		{"negative once truncated to int", 1 << 63, false},
		{"cpu bomb", 1 << 40, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := Decode(encodeWithProbes(tt.k, 64))
			if !tt.ok {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, int(tt.k), got.k)
		})
	}
}

func TestNewClampsProbes(t *testing.T) {
	t.Parallel()

	f := New(1, 1e-40)
	assert.LessOrEqual(t, f.k, maxProbes, "a built filter always decodes back")
	assert.Positive(t, f.k)

	got, _, err := Decode(f.Encode(nil))
	require.NoError(t, err)
	assert.Equal(t, f.k, got.k)
}

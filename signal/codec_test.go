package signal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []Value{
		EmptyValue(),
		sv("hello"),
		sv(""),
		BoolValue(true),
		BoolValue(false),
		IntValue(-42),
		DoubleValue(3.14159),
		BytesValue([]byte{0, 1, 2, 255}),
		SliceValue(IntValue(1), sv("x"), BoolValue(true)),
		MapValue(kv("a", IntValue(1)), kv("b", sv("v"))),
		SliceValue(MapValue(kv("nested", SliceValue(IntValue(9))))),
	}
	for _, want := range cases {
		got, n, err := DecodeValue(AppendValue(nil, want))
		require.NoError(t, err)
		assert.True(t, want.Equal(got), "%v != %v", text(want), text(got))
		assert.Positive(t, n)
	}
}

func TestAttributesBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	a := NewAttributes(
		kv("job", sv("api")),
		kv("count", IntValue(7)),
		kv("ratio", DoubleValue(0.5)),
		kv("tags", SliceValue(sv("x"), sv("y"))),
		kv("meta", MapValue(kv("k", BytesValue([]byte("v"))))),
	)

	got, n, err := DecodeAttributes(a.AppendHashInput(nil))
	require.NoError(t, err)
	assert.True(t, a.Equal(got))
	assert.Positive(t, n)
	// The decoded set hashes to the same SeriesID.
	assert.Equal(t, a.Hash(), got.Hash())
}

// TestAppendAttributesReuse decodes several blobs through one reused buffer (the row-by-row
// materialization pattern) and asserts each result matches a fresh DecodeAttributes.
func TestAppendAttributesReuse(t *testing.T) {
	t.Parallel()

	sets := []Attributes{
		NewAttributes(kv("job", sv("api")), kv("n", IntValue(1))),
		NewAttributes(kv("region", sv("eu")), kv("cpu", sv("0")), kv("mode", sv("user"))),
		NewAttributes(kv("only", sv("one"))),
	}

	buf := make(Attributes, 0, 8)
	for _, want := range sets {
		blob := want.AppendHashInput(nil)

		got, n, err := AppendAttributes(buf[:0], blob)
		require.NoError(t, err)
		assert.Positive(t, n)
		assert.True(t, want.Equal(got), "reused-buffer decode equals the source set")
		assert.Equal(t, want.Hash(), got.Hash())

		buf = got // carry the (grown) backing array to the next iteration
	}
}

func TestLookupAttribute(t *testing.T) {
	t.Parallel()

	a := NewAttributes(
		kv("job", sv("api")),
		kv("count", IntValue(7)),
		kv("ratio", DoubleValue(0.5)),
	)
	blob := a.AppendHashInput(nil)

	// Present keys agree with DecodeAttributes().Get; an absent key reports false.
	for _, key := range []string{"job", "count", "ratio"} {
		v, ok, err := LookupAttribute(blob, key)
		require.NoError(t, err)
		require.True(t, ok)
		want, _ := a.Get([]byte(key))
		assert.Truef(t, want.Equal(v), "LookupAttribute(%q) matches Get", key)
	}

	_, ok, err := LookupAttribute(blob, "missing")
	require.NoError(t, err)
	assert.False(t, ok)

	// Truncations never panic.
	for n := range blob {
		_, _, _ = LookupAttribute(blob[:n], "job")
	}
}

//nolint:paralleltest // testing.AllocsPerRun must not run during a parallel test.
func TestLookupAttributeZeroAlloc(t *testing.T) {
	blob := NewAttributes(kv("user", sv("alice")), kv("region", sv("eu"))).AppendHashInput(nil)

	allocs := testing.AllocsPerRun(100, func() {
		_, _, _ = LookupAttribute(blob, "region")
	})
	assert.Zero(t, allocs, "a targeted attribute lookup materializes no slice")
}

func FuzzLookupAttribute(f *testing.F) {
	f.Add(NewAttributes(kv("k", sv("v"))).AppendHashInput(nil), "k")
	f.Add([]byte{0x00}, "x")

	f.Fuzz(func(t *testing.T, blob []byte, key string) {
		// Must never panic; when it succeeds, it must agree with a full decode.
		v, ok, err := LookupAttribute(blob, key)
		if err != nil {
			return
		}

		a, _, derr := DecodeAttributes(blob)
		if derr != nil {
			return // LookupAttribute may accept a prefix DecodeAttributes rejects; nothing to compare
		}

		want, wantOK := a.Get([]byte(key))
		assert.Equal(t, wantOK, ok)
		if ok {
			assert.True(t, want.Equal(v))
		}
	})
}

func TestDecodeRejectsTruncation(t *testing.T) {
	t.Parallel()

	full := AppendValue(nil, SliceValue(IntValue(1), sv("abc")))
	for n := range full {
		_, _, err := DecodeValue(full[:n])
		require.Errorf(t, err, "prefix %d should be rejected", n)
	}

	_, _, err := DecodeValue([]byte{0xFF}) // unknown kind
	require.ErrorIs(t, err, ErrMalformed)
}

func FuzzDecodeAttributes(f *testing.F) {
	f.Add(NewAttributes(kv("job", sv("api")), kv("n", IntValue(1))).AppendHashInput(nil))
	f.Add([]byte{})
	f.Add([]byte{0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		a, _, err := DecodeAttributes(data)
		if err != nil {
			return
		}
		// Accepted ⇒ re-encode round-trips and hashes stably.
		got, _, err := DecodeAttributes(a.AppendHashInput(nil))
		require.NoError(t, err)
		assert.True(t, a.Equal(got))
		assert.Equal(t, a.Hash(), got.Hash())
	})
}

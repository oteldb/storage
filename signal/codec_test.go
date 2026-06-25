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

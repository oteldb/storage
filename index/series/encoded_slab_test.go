package series

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestEncodedDescriptorSlabsPreserveViews(t *testing.T) {
	t.Parallel()
	ix := New()
	descriptor := func(value []byte) []byte {
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("value"), Value: signal.StringValue(value),
		})}
		return s.AppendHashInput(nil)
	}

	first := descriptor([]byte("initial"))
	expected := bytes.Clone(first)
	id := signal.HashBytes(first)
	ref, inserted := ix.InternEncoded(id, first)
	require.True(t, inserted)
	v := ix.View()
	clear(first)

	for _, n := range []int{encodedSlabBytes / 2, 3 * encodedSlabBytes / 4, 2 * encodedSlabBytes} {
		raw := descriptor(bytes.Repeat([]byte("x"), n))
		want := bytes.Clone(raw)
		ref := ix.AddEncoded(signal.HashBytes(raw), raw)
		clear(raw)
		got := ix.View().EncodedDescriptor(int(ref))
		require.Equal(t, want, got)
		require.Equal(t, len(got), cap(got))
	}
	require.Equal(t, expected, v.EncodedDescriptor(int(ref)))
	duplicate, inserted := ix.InternEncoded(id, expected)
	require.Equal(t, ref, duplicate)
	require.False(t, inserted)
	require.Len(t, ix.rows, 4)
	require.GreaterOrEqual(t, ix.encodedBytes, int64(4*encodedSlabBytes))
	require.GreaterOrEqual(t, ix.SizeBytes(), ix.encodedBytes)
}

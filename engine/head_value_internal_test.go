package engine

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

func TestHeadInternValueOwnsBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		v    signal.Value
	}{
		{"empty", signal.EmptyValue()},
		{"string", signal.StringValue([]byte("node-12345:9100"))},
		{"long", signal.StringValue(bytes.Repeat([]byte("x"), 1024))},
		{"bytes", signal.BytesValue([]byte{0, 1, 255})},
		{"boolean", signal.BoolValue(true)},
		{"integer", signal.IntValue(-12345)},
		{"double", signal.DoubleValue(math.Float64frombits(0x7ff8000000000001))},
		{"nested", signal.MapValue(signal.KeyValue{Key: []byte("roles"), Value: signal.SliceValue(signal.BoolValue(true), signal.IntValue(10))})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHead()
			before := h.identityBytes()
			id := h.internValue(tc.v)
			want := signal.AppendValue(nil, tc.v)
			require.GreaterOrEqual(t, h.identityBytes()-before, int64(cap(h.valueScratch)))
			clear(h.valueScratch)
			h.internValue(signal.StringValue(bytes.Repeat([]byte("y"), len(want)+1)))
			got, found := h.sym.Get(symbols.ID(id))
			require.True(t, found)
			require.Equal(t, want, got, "interned bytes must not alias encoding scratch")
			require.Equal(t, id, h.internValue(tc.v))
		})
	}
}

func TestHeadValueScratchAccounting(t *testing.T) {
	t.Parallel()
	h := newHead()
	before := h.identityBytes()
	h.valueScratch = make([]byte, 0, 128)
	require.Equal(t, before+128, h.identityBytes())
}

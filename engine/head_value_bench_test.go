package engine

import (
	"bytes"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkHeadInternValue(b *testing.B) {
	for _, tc := range []struct {
		name string
		v    signal.Value
	}{
		{"host", signal.StringValue([]byte("node-12345:9100"))},
		{"long", signal.StringValue(bytes.Repeat([]byte("x"), 1024))},
		{"integer", signal.IntValue(12345)},
		{"nested", signal.MapValue(signal.KeyValue{Key: []byte("roles"), Value: signal.SliceValue(signal.BoolValue(true), signal.IntValue(10))})},
	} {
		b.Run(tc.name, func(b *testing.B) {
			h := newHead()
			want := h.internValue(tc.v)
			b.ReportAllocs()
			for b.Loop() {
				if got := h.internValue(tc.v); got != want {
					b.Fatal("interned identity changed")
				}
			}
		})
	}
}

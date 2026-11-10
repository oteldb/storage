package series

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkViewCanonical(b *testing.B) {
	cases := []struct {
		name     string
		original signal.Series
	}{
		{name: "flat", original: mk("node-exporter", "__name__", "node_cpu_seconds_total", "instance", "host-0", "cpu", "0", "mode", "idle")},
		{name: "nested", original: encodingSeries()},
	}
	for i := range cases {
		tc := &cases[i]
		ix := New()
		ix.Add(tc.original)
		view := ix.View()
		b.Run(tc.name, func(b *testing.B) {
			b.Run("materialized", func(b *testing.B) {
				var buf []byte
				var attrs signal.Attributes
				b.ReportAllocs()
				for b.Loop() {
					s := view.At(0, attrs)
					buf = s.AppendHashInput(buf[:0])
					attrs = s.Attributes
				}
			})
			b.Run("encoded", func(b *testing.B) {
				var buf []byte
				b.ReportAllocs()
				for b.Loop() {
					buf = view.AppendCanonical(buf[:0], 0)
				}
			})
		})
	}
}

func BenchmarkIndexCopyView(b *testing.B) {
	const count = 4096
	nestedValue := signal.MapValue(signal.KeyValue{Key: []byte("samples"), Value: signal.SliceValue(signal.IntValue(1), sv("1"))})
	for _, nested := range []bool{false, true} {
		source := New()
		for i := range count {
			s := mk("node-exporter", "__name__", "node_cpu_seconds_total", "instance", strconv.Itoa(i), "cpu", "0", "mode", "idle")
			if nested {
				s.Attributes = append(s.Attributes, signal.KeyValue{Key: []byte("nested"), Value: nestedValue})
			}
			source.Add(s)
		}
		view := source.View()
		b.Run("nested="+strconv.FormatBool(nested), func(b *testing.B) {
			b.Run("materialized", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					ix := New()
					var attrs signal.Attributes
					for i := range view.Len() {
						s := view.At(i, attrs)
						ix.Add(s)
						attrs = s.Attributes
					}
				}
			})
			b.Run("encoded", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					ix := New()
					for i := range view.Len() {
						ix.AddView(view, i)
					}
				}
			})
		})
	}
}

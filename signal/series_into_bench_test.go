package signal

import "testing"

func BenchmarkDecodeSeriesInto(b *testing.B) {
	cases := seriesIntoCases()
	for i := range cases {
		tc := &cases[i]
		b.Run(tc.name, func(b *testing.B) {
			raw := tc.identity.AppendHashInput(nil)
			b.Run("fresh", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				for b.Loop() {
					_, _, err := DecodeSeries(raw)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("reuse", func(b *testing.B) {
				var dst Series
				if _, err := DecodeSeriesInto(&dst, raw); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				for b.Loop() {
					if _, err := DecodeSeriesInto(&dst, raw); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

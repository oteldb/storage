package chunk

import "testing"

func BenchmarkEncodeU128RunsCapacity(b *testing.B) {
	for _, tc := range []struct {
		name       string
		runs, rows int
		mode       string
	}{
		{"single", 1, 16384, "distinct"},
		{"separated", 1024, 16, "distinct"},
		{"dense", 32768, 2, "distinct"},
		{"alternating", 32768, 1, "alternating"},
		{"coalesced", 32768, 1, "equal"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runs := make([]U128Run, tc.runs)
			for i := range runs {
				id := U128{Hi: uint64(i), Lo: uint64(i)*0x9e3779b97f4a7c15 + 1}
				switch tc.mode {
				case "alternating":
					id = U128{Lo: uint64(i % 2)}
				case "equal":
					id = U128{Lo: 1}
				}
				runs[i] = U128Run{Value: id, Count: tc.rows}
			}
			b.Run("fresh", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(tc.runs) * int64(tc.rows) * 16)
				for b.Loop() {
					_ = EncodeU128Runs(nil, runs)
				}
			})
			b.Run("reuse", func(b *testing.B) {
				dst := EncodeU128Runs(nil, runs)
				b.ReportAllocs()
				b.SetBytes(int64(tc.runs) * int64(tc.rows) * 16)
				for b.Loop() {
					dst = EncodeU128Runs(dst[:0], runs)
				}
			})
		})
	}
}

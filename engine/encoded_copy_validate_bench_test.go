package engine

import (
	"fmt"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// BenchmarkEncodedGranuleChecks isolates the CPU costs of checking already resident inner bytes.
func BenchmarkEncodedGranuleChecks(b *testing.B) {
	for _, rows := range []int{16, 256} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			times := make([]int64, rows)
			values := make([]float64, rows)
			for i := range times {
				times[i] = 1_700_000_000_000 + int64(i)*15_000
				values[i] = math.Sin(float64(i)) * 100
			}
			ts, err := chunk.EncodeTimestampsAdaptive(nil, times)
			require.NoError(b, err)
			var encoder chunk.FloatGroupEncoder
			vs, err := encoder.Encode(nil, values, []int{rows})
			require.NoError(b, err)
			tab := crc32.MakeTable(crc32.Castagnoli)
			tsCRC, vCRC := crc32.Checksum(ts, tab), crc32.Checksum(vs, tab)
			irregular := make([]int64, rows)
			for i := range irregular {
				irregular[i] = times[i] + int64((i*17)%23)
			}
			jitter, err := chunk.EncodeTimestampsAdaptive(nil, irregular)
			require.NoError(b, err)
			for _, tc := range []struct {
				name   string
				raw    []byte
				points []int64
			}{
				{"regular", ts, times},
				{"jitter", jitter, irregular},
			} {
				b.Run("timestamp-semantics/"+tc.name, func(b *testing.B) {
					b.SetBytes(int64(len(tc.raw)))
					for b.Loop() {
						lo, hi, ordered, err := chunk.ValidateTimestampsAdaptiveRows(tc.raw, rows)
						if err != nil || !ordered || lo != tc.points[0] || hi != tc.points[rows-1] {
							b.Fatalf("timestamp bounds [%d,%d], ordered=%t: %v", lo, hi, ordered, err)
						}
					}
				})
			}
			b.Run("checksums", func(b *testing.B) {
				b.SetBytes(int64(len(ts) + len(vs)))
				for b.Loop() {
					if crc32.Checksum(ts, tab) != tsCRC || crc32.Checksum(vs, tab) != vCRC {
						b.Fatal("checksum mismatch")
					}
				}
			})
			b.Run("semantics-and-checksums", func(b *testing.B) {
				b.SetBytes(int64(len(ts) + len(vs)))
				for b.Loop() {
					lo, hi, ordered, err := chunk.ValidateTimestampsAdaptiveRows(ts, rows)
					if err != nil || !ordered || lo != times[0] || hi != times[rows-1] {
						b.Fatalf("timestamp bounds [%d,%d], ordered=%t: %v", lo, hi, ordered, err)
					}
					if err := chunk.ValidateFloatGroupsRows(vs, rows); err != nil {
						b.Fatal(err)
					}
					if crc32.Checksum(ts, tab) != tsCRC || crc32.Checksum(vs, tab) != vCRC {
						b.Fatal("checksum mismatch")
					}
				}
			})
		})
	}
}

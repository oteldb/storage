package chunk

import (
	"encoding/binary"
	"testing"
)

// makeIDColumn builds rows 16-byte ids drawn from distinct values, each repeated round-robin — the
// shape of a trace_id column (one row per span; a trace's spans share an id). distinct==rows gives
// unique-per-row; distinct<rows gives repetition.
func makeIDColumn(rows, distinct int) [][]byte {
	out := make([][]byte, rows)
	for i := range rows {
		id := make([]byte, 16)
		binary.BigEndian.PutUint64(id[0:8], 0xABCD)
		binary.BigEndian.PutUint64(id[8:16], uint64(i%distinct))
		out[i] = id
	}

	return out
}

// BenchmarkBytesIDColumn compares Dict vs BytesRaw on 16-byte id columns across cardinality. It
// reports encode throughput (logical id bytes/sec) and the encoded size as bytes/row, so the
// dictionary codec's 65536-distinct fallback is visible: below the cap Dict deduplicates repeats;
// above it (the trace-store norm) Dict degrades to its flat 17-byte/row form while BytesRaw stays
// fixed-width at 16 bytes/row.
func BenchmarkBytesIDColumn(b *testing.B) {
	const rows = 200_000

	cases := []struct {
		name     string
		distinct int
	}{
		{"LowCard_1k", 1_000},      // ≤65536 distinct: Dict dictionary-encodes
		{"HighCard_unique", rows},  // all distinct ≫ 65536: Dict → flat fallback
		{"HighCard_100k", 100_000}, // > 65536 distinct with light repetition
	}

	codecs := []struct {
		name   string
		encode func(dst []byte, vals [][]byte) []byte
	}{
		{"Dict", EncodeBytes},
		{"Raw", EncodeBytesRaw},
	}

	for _, tc := range cases {
		vals := makeIDColumn(rows, tc.distinct)
		logical := int64(rows) * 16

		for _, cd := range codecs {
			b.Run(tc.name+"/"+cd.name, func(b *testing.B) {
				b.SetBytes(logical)
				b.ReportAllocs()

				var enc []byte
				for b.Loop() {
					enc = cd.encode(enc[:0], vals)
				}

				b.ReportMetric(float64(len(enc))/float64(rows), "bytes/row")
			})
		}
	}
}

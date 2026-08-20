package block

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
)

// Decode cost of a bytes column, single-stream against block-framed. Framing exists to let a query
// decode a fraction of a column, so the numbers that matter are two: what a *whole*-column decode
// pays for the framing (the regression risk, since framing adds a directory walk and a decompression
// per frame instead of one per column), and what a *pruned* decode saves (the point of it).
//
// Both sides report throughput over the logical uncompressed bytes — the sum of the values' lengths
// — so blocked and unblocked are directly comparable, and a pruned decode is sized by the bytes it
// actually produced rather than the column's, which is what makes its speedup legible as throughput
// rather than as a smaller number.

// benchVals builds rows values with the given number of distinct ones, cycling — the low-distinct
// shape of a record-attributes column and, at distinct == rows, the near-unique shape of a log body.
func benchVals(rows, distinct int) [][]byte {
	out := make([][]byte, rows)
	for i := range rows {
		out[i] = fmt.Appendf(nil, "value-%d-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", i%distinct)
	}

	return out
}

func logicalBytes(vals [][]byte) int64 {
	var n int64
	for _, v := range vals {
		n += int64(len(v))
	}

	return n
}

// benchColumn builds the column in the requested layout. When blocked, it constructs the framed form
// directly rather than going through [buildColumn], which grants framing only where it pays and would
// otherwise hand back a single stream for the near-unique shapes below — leaving the benchmark
// comparing a column against itself and the pruned cases with no granules to select. Forcing it is
// what keeps the trade-off this file exists to measure visible.
func benchColumn(b *testing.B, vals [][]byte, blocked bool, granule int) *ColumnReader {
	b.Helper()

	c := Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: blocked}

	if !blocked {
		desc, obj, err := buildColumn(c, zstdComp(), granule, defaultCompressBlockBytes)
		if err != nil {
			b.Fatal(err)
		}

		return newColumnReader(desc, obj, zstdComp(), len(vals))
	}

	desc := ColumnDesc{
		Name: "c", Kind: KindBytes, Codec: chunk.CodecDict,
		Compress: zstdComp().Algorithm(), Level: zstdComp().Level(), Blocked: true, Framed: true,
		Checked: true,
	}

	obj, ok, err := trySharedDict(c, chunk.CodecDict, zstdComp(), granule, defaultCompressBlockBytes)
	if err != nil {
		b.Fatal(err)
	}

	if ok {
		desc.SharedDict = true

		return newColumnReader(desc, obj, zstdComp(), len(vals))
	}

	obj, err = encodeBlocked(c, chunk.CodecDict, 0, zstdComp(), granule, defaultCompressBlockBytes)
	if err != nil {
		b.Fatal(err)
	}

	return newColumnReader(desc, obj, zstdComp(), len(vals))
}

func BenchmarkBytesColumnDecode(b *testing.B) {
	const (
		rows    = 1 << 17
		granule = 8192
	)

	nBlocks := rows / granule

	for _, shape := range []struct {
		name     string
		distinct int
	}{
		{"lowcard", 32},    // a record-attributes column: blobs repeating across the whole part
		{"highcard", rows}, // a log body: near-unique per row
	} {
		vals := benchVals(rows, shape.distinct)
		total := logicalBytes(vals)

		b.Run(shape.name, func(b *testing.B) {
			b.Run("unblocked", func(b *testing.B) {
				r := benchColumn(b, vals, false, granule)

				b.SetBytes(total)
				b.ReportAllocs()

				for b.Loop() {
					if _, err := r.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("blocked/all", func(b *testing.B) {
				r := benchColumn(b, vals, true, granule)

				b.SetBytes(total)
				b.ReportAllocs()

				for b.Loop() {
					if _, err := r.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			// Pruned decodes: the fraction of granules a narrow time window touches. Sized by the
			// bytes produced, so the number is a decode speed and not a smaller total in disguise.
			for _, want := range []int{nBlocks / 8, 1} {
				blocks := make([]int, want)
				for i := range blocks {
					blocks[i] = i * (nBlocks / want)
				}

				b.Run(fmt.Sprintf("blocked/%dof%d", want, nBlocks), func(b *testing.B) {
					r := benchColumn(b, vals, true, granule)

					b.SetBytes(total * int64(want) / int64(nBlocks))
					b.ReportAllocs()

					for b.Loop() {
						if _, err := r.DecodeBlocksBytesIntoColumn(blocks); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

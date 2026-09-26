package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

// TestSidecarCompression reports each table of a real CPU profile raw against compressed, and pins
// that the sidecar compressor pays for itself on the stacks.
func TestSidecarCompression(t *testing.T) {
	t.Parallel()

	corpus := pprofCorpus(t)

	var rawTotal, zstdTotal int

	for i, name := range tableNames {
		raw := len(encodeTable(corpus.t[i], memoryCompressor))
		packed := len(encodeTable(corpus.t[i], storageCompressor))
		rawTotal += raw
		zstdTotal += packed

		t.Logf("%-9s %6d entries  raw %8d B  zstd %7d B  %.2f×",
			name, len(corpus.t[i]), raw, packed, float64(raw)/float64(packed))
	}

	t.Logf("%-9s                 raw %8d B  zstd %7d B  %.2f×",
		"total", rawTotal, zstdTotal, float64(rawTotal)/float64(zstdTotal))

	stacks := corpus.t[4]
	require.NotEmpty(t, stacks)
	assert.Less(t, 3*len(encodeTable(stacks, storageCompressor)), len(encodeTable(stacks, memoryCompressor)),
		"stacks compress at least 3×")
}

func BenchmarkSymbolTable(b *testing.B) {
	corpus := pprofCorpus(b)

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
		c := tableCompressor(alg)

		for i, name := range tableNames {
			table := corpus.t[i]
			logical := int64(len(encodeTable(table, memoryCompressor)))
			enc := encodeTable(table, c)

			b.Run(alg.String()+"/"+name+"/Encode", func(b *testing.B) {
				b.SetBytes(logical)
				b.ReportAllocs()

				for b.Loop() {
					encodeTable(table, c)
				}
			})

			b.Run(alg.String()+"/"+name+"/Decode", func(b *testing.B) {
				b.SetBytes(logical)
				b.ReportAllocs()

				for b.Loop() {
					if err := decodeTable(make(map[signal.SeriesID][]byte, len(table)), enc); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

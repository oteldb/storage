package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

func TestSampleMergeCompressionTier(t *testing.T) {
	t.Parallel()
	cold := &RecompressSpec{Before: 100, Algorithm: compress.AlgorithmZSTD, Level: compress.LevelBest}
	for _, tc := range []struct {
		name   string
		spec   *RecompressSpec
		newest int64
		rows   int
		sparse bool
		want   compressProfile
	}{
		{name: "small sparse", rows: ladderRowsMedium, sparse: true, want: compressProfile{}},
		{name: "large sparse", rows: ladderRowsMedium + 1, sparse: true, want: compressProfile{compress.AlgorithmZSTD, 3}},
		{name: "small dense", rows: 128, want: compressProfile{compress.AlgorithmZSTD, 1}},
		{name: "exact cold cutoff", spec: cold, newest: 100, rows: 128, sparse: true, want: compressProfile{}},
		{
			name: "cold override", spec: cold, newest: 99, rows: 128, sparse: true,
			want: compressProfile{compress.AlgorithmZSTD, compress.LevelBest},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, sampleMergeProfile(tc.spec, tc.newest, tc.rows, tc.sparse))
		})
	}
}

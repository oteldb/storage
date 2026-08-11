package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/encoding/compress"
)

func TestLadderLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		rows int
		want compress.Level
	}{
		{0, 1},
		{1, 1},
		{ladderRowsFast, 1},
		{ladderRowsFast + 1, 2},
		{ladderRowsMedium, 2},
		{ladderRowsMedium + 1, 3},
		{1 << 30, 3},
	} {
		assert.Equalf(t, tc.want, ladderLevel(tc.rows), "ladderLevel(%d)", tc.rows)
	}
}

func TestMergeProfile(t *testing.T) {
	t.Parallel()

	cold := &RecompressSpec{Before: 1000, Algorithm: compress.AlgorithmZSTD, Level: 9}

	for _, tc := range []struct {
		name    string
		spec    *RecompressSpec
		maxTime int64
		rows    int
		want    compressProfile
	}{
		{
			name: "no policy takes the ladder",
			rows: 10, want: compressProfile{compress.AlgorithmZSTD, 1},
		},
		{
			name: "big part climbs the ladder",
			rows: ladderRowsMedium + 1, want: compressProfile{compress.AlgorithmZSTD, 3},
		},
		{
			name: "warm part ignores the cold tier",
			spec: cold, maxTime: 5000, rows: 10, want: compressProfile{compress.AlgorithmZSTD, 1},
		},
		{
			name: "cold part takes the age tier",
			spec: cold, maxTime: 500, rows: 10, want: compressProfile{compress.AlgorithmZSTD, 9},
		},
		{
			name:    "cold tier below the ladder does not downgrade it",
			spec:    &RecompressSpec{Before: 1000, Algorithm: compress.AlgorithmZSTD, Level: 1},
			maxTime: 500, rows: ladderRowsMedium + 1, want: compressProfile{compress.AlgorithmZSTD, 3},
		},
		{
			name:    "a different algorithm always wins",
			spec:    &RecompressSpec{Before: 1000, Algorithm: compress.AlgorithmLZ4, Level: 1},
			maxTime: 500, rows: 10, want: compressProfile{compress.AlgorithmLZ4, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, mergeProfile(tc.spec, tc.maxTime, tc.rows))
		})
	}
}

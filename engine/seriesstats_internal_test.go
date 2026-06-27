package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func TestComputeAndRoundTripSeriesStats(t *testing.T) {
	t.Parallel()

	cols := &flushColumns{
		series: []chunk.U128{{Lo: 1}, {Lo: 1}, {Lo: 1}, {Lo: 2}},
		ts:     []int64{1, 2, 3, 9},
		value:  []float64{10, 20, 6, 5},
	}

	ids, stats := computeSeriesStats(cols)
	require.Len(t, ids, 2)

	enc := encodeSeriesStats(ids, stats)
	got, err := decodeSeriesStats(enc)
	require.NoError(t, err)

	assert.Equal(t, SeriesAgg{Count: 3, Sum: 36, Min: 6, Max: 20}, got[signal.SeriesID{Lo: 1}])
	assert.Equal(t, SeriesAgg{Count: 1, Sum: 5, Min: 5, Max: 5}, got[signal.SeriesID{Lo: 2}])
}

func TestDecodeSeriesStatsRejectsCorruption(t *testing.T) {
	t.Parallel()

	_, err := decodeSeriesStats([]byte("not a sidecar"))
	require.ErrorIs(t, err, errStatsCorrupt)

	enc := encodeSeriesStats([]chunk.U128{{Lo: 7}}, []SeriesAgg{{Count: 1, Sum: 1, Min: 1, Max: 1}})
	enc[len(enc)-1] ^= 0xff // corrupt the CRC
	_, err = decodeSeriesStats(enc)
	require.ErrorIs(t, err, errStatsCorrupt)
}

func TestSeriesAggMerge(t *testing.T) {
	t.Parallel()

	a := SeriesAgg{Count: 2, Sum: 30, Min: 10, Max: 20}
	a.merge(SeriesAgg{Count: 1, Sum: 5, Min: 5, Max: 5})
	assert.Equal(t, SeriesAgg{Count: 3, Sum: 35, Min: 5, Max: 20}, a)

	a.merge(SeriesAgg{}) // empty is a no-op
	assert.Equal(t, int64(3), a.Count)
}

package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

func TestMergeSeriesReservationBound(t *testing.T) {
	t.Parallel()
	src := []*part{{index: partIndex{ids: make([]signal.SeriesID, 80000)}}}
	for _, allowance := range []int64{0, 1024, 32 << 20, 256 << 20} {
		for _, stats := range []bool{false, true} {
			r := newMergeSeriesReservation(src, allowance, 1024)
			original := r.remaining
			var allocated int64
			for range 4 {
				p := &partStreamWriter{e: New(Config{}), w: block.NewStreamWriter(), withStats: stats}
				require.NoError(t, p.w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128}))
				before := p.residentBytes()
				require.NoError(t, r.reserve(p))
				require.Empty(t, p.runs)
				require.Empty(t, p.stats)
				require.Zero(t, p.w.Rows())
				allocated += p.residentBytes() - before
				require.GreaterOrEqual(t, r.remaining, int64(0))
				require.LessOrEqual(t, allocated, original)
			}
			require.LessOrEqual(t, original, int64(32<<20))
			if allowance > 0 {
				require.LessOrEqual(t, original, max(allowance-1024, 0)/8)
			}
		}
	}
}

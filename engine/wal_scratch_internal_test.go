package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestWALStageReleasesBorrowedReferences(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, fsys := newFaultWALEngine(t, 0)
			if fail {
				failNextWALWrite(fsys)
			}
			s := stagedTestSeries("borrowed")
			id := s.Hash()
			_, err := e.AppendBatch([]signal.SeriesID{id}, []int64{10}, []float64{1}, nil,
				func(int) signal.Series { return s }, AppendLimits{})
			if fail {
				require.ErrorIs(t, err, errInjectedWAL)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, e.walB.accs, 1)
			acc := &e.walB.accs[0]
			assert.Equal(t, signal.Series{}, acc.series)
			assert.Nil(t, acc.buffer)
			ts, values, weights := cap(acc.ts), cap(acc.values), cap(acc.sf)
			_, err = e.Append(s, 20, 2)
			require.NoError(t, err)
			assert.Equal(t, signal.Series{}, acc.series)
			assert.Nil(t, acc.buffer)
			assert.Equal(t, ts, cap(acc.ts))
			assert.Equal(t, values, cap(acc.values))
			assert.Equal(t, weights, cap(acc.sf))
		})
	}
}

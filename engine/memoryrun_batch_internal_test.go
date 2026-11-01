package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestMemoryRunOwnsAcceptedWALData(t *testing.T) {
	t.Parallel()
	h := newHead()
	batch := newWALBatch()
	s := apiSeries()
	for _, point := range []struct {
		ts        int64
		value, sf float64
	}{{20, 1, 1}, {10, 2, 3}, {10, 3, 2}} {
		outcome := batch.admit(h, s.Hash(), point.ts, point.value, point.sf, 0, AppendLimits{}, func() signal.Series { return s })
		require.Equal(t, admitted, outcome)
	}
	stagedTS := batch.accs[0].ts
	stagedValues := batch.accs[0].values
	released := false
	run, err := memoryRunFromBatch(batch, func() { released = true })
	require.NoError(t, err)
	require.Equal(t, []int64{20, 10, 10}, stagedTS, "WAL arrival order is untouched")
	rng, found := run.lookup(s.Hash())
	require.True(t, found)
	require.Equal(t, []int64{10, 10, 20}, run.ts[rng.start:rng.end])
	require.Equal(t, []float64{2, 3, 1}, run.values[rng.start:rng.end])
	require.Equal(t, []float64{3, 2, 1}, run.sf[rng.start:rng.end])
	batch.reset()
	for i := range stagedTS {
		stagedTS[i] = -100
		stagedValues[i] = -100
	}
	require.Equal(t, []int64{10, 10, 20}, run.ts)
	require.Equal(t, []float64{2, 3, 1}, run.values)
	var merge sampleMerge
	merge.add(run.ts, run.values, run.sf, nil, minInt64, maxInt64)
	ts, values, sf := merge.collect(nil, nil)
	require.Equal(t, []int64{10, 20}, ts)
	require.Equal(t, []float64{3, 1}, values)
	require.Equal(t, []float64{2, 1}, sf)
	run.release()
	require.True(t, released)
}

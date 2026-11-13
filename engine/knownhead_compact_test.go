package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func TestKnownHeadCompactMixed(t *testing.T) {
	t.Parallel()
	for _, active := range []int{0, 1, 3} {
		t.Run(string(rune('0'+active)), func(t *testing.T) {
			t.Parallel()
			h := newHead()
			h.disk = newDiskHeadState()
			snapshot := newSeriesSnapshot(nil)
			marks := map[signal.SeriesID]int64{}
			for i := range 3 {
				id := signal.SeriesID{Lo: uint64(i)}
				marks[id] = int64(100 + i)
				if i < active {
					h.samples[id] = &sampleBuf{ts: []int64{100}, values: []float64{1}}
				}
			}
			for i := range 8192 {
				h.samples[signal.SeriesID{Hi: 1, Lo: uint64(i)}] = &sampleBuf{}
			}
			h.known = &knownSampleHead{snapshot: snapshot, marks: marks}
			defer h.closeKnownSamples()
			h.compactKnownSamples()
			require.Equal(t, active, h.known.len())
			if active == 0 {
				require.Nil(t, h.known)
				require.Zero(t, snapshot.refs.Load())
				return
			}
			require.Len(t, h.known.marks, active)
			require.Less(t, h.known.sizeBytes(), int64(2048), "unknown sample IDs do not size the compact registry")
			require.EqualValues(t, active, h.known.rows)
			if active == 3 {
				marks[signal.SeriesID{}] = 999
				require.EqualValues(t, 999, h.known.marks[signal.SeriesID{}], "all surviving keys keep the existing map")
			}
		})
	}
}

func TestKnownHeadCatalogCompareBatches(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	seedKnownSelection(t, e, 300)
	identities := seedKnownSelection(t, e, 600)
	ids := make([]signal.SeriesID, len(identities))
	ts := make([]int64, len(ids))
	values := make([]float64, len(ids))
	for i, s := range identities {
		ids[i] = s.Hash()
		ts[i] = 100
		values[i] = 2
	}
	_, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{})
	require.NoError(t, err)
	old := e.head.known.snapshot
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotSame(t, old, e.seriesTable)
	budget := readbudget.New(64 << 20)
	ctx := readbudget.With(t.Context(), budget)
	require.NoError(t, compareKnownCatalog(ctx, old, e.seriesTable, ids))
	require.EqualValues(t, 64<<20, budget.Remaining())
	err = compareKnownBatch(ctx, e.seriesTable, ids[:1], func(signal.SeriesID) []byte { return []byte("different canonical bytes") })
	require.ErrorContains(t, err, "canonical identity changed")
	require.EqualValues(t, 64<<20, budget.Remaining())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, compareKnownCatalog(canceled, old, e.seriesTable, ids), context.Canceled)
	require.EqualValues(t, 64<<20, budget.Remaining())
	require.NoError(t, e.Flush(t.Context()))
	count, err := e.Count(t.Context(), fetch.Request{Start: 90, End: 110})
	require.NoError(t, err)
	require.Equal(t, len(ids), count)
}

func TestKnownHeadCatalogCompareLargeIdentity(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	large := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("payload"), Value: signal.StringValue([]byte(strings.Repeat("x", knownCatalogBatchBytes+1)))})}
	knownHeadAppend(t, e, large, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, benchSeries(1), 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, large, 100, 2)
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	require.NoError(t, e.verifyKnownCatalog(t.Context(), []signal.SeriesID{large.Hash()}))
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
}

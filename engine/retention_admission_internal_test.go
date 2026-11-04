package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestTimestampFloorBeforeIdentityAndWAL(t *testing.T) {
	t.Parallel()
	for _, logged := range []bool{false, true} {
		name := "memory"
		if logged {
			name = "wal"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := New(Config{})
			var replay func() *head
			if logged {
				durable, fs := newFaultWALEngine(t, 0)
				e = durable
				replay = func() *head { return replayedHead(t, fs) }
			}
			expired, valid := stagedTestSeries("expired"), stagedTestSeries("valid")
			ids := []signal.SeriesID{expired.Hash(), valid.Hash(), valid.Hash()}
			limits := AppendLimits{MinTimestamp: 10, EnforceMinTimestamp: true, MaxSeries: 1}
			result, err := e.AppendBatch(ids, []int64{9, 10, 11}, []float64{1, 2, 3}, nil, func(i int) signal.Series {
				require.NotZero(t, i, "expired identity must stay unmaterialized")
				return valid
			}, limits)
			require.NoError(t, err)
			require.Equal(t, AppendResult{Accepted: 2, RejectedRetention: 1}, result)
			require.EqualValues(t, 1, e.Stats().Series)
			require.Equal(t, []int64{10, 11}, headTS(e.head, valid))
			if replay != nil {
				h := replay()
				require.Equal(t, []int64{10, 11}, headTS(h, valid))
				require.False(t, h.series.Has(expired.Hash()))
			}
		})
	}
}

func TestTimestampFloorPrimaryReplication(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	expired, valid := stagedTestSeries("expired"), stagedTestSeries("valid")
	payload := primaryPayload(t, seriesRun(expired, []int64{-1}, []float64{1}), seriesRun(valid, []int64{0, 1}, []float64{2, 3}))
	accepted, res, err := e.ApplyPrimary(payload, AppendLimits{MinTimestamp: 0, EnforceMinTimestamp: true, MaxSeries: 1})
	require.NoError(t, err)
	require.Equal(t, AppendResult{Accepted: 2, RejectedRetention: 1}, res)
	replica := New(Config{})
	require.NoError(t, replica.ApplyReplicated(accepted))
	for _, h := range []*head{e.head, replica.head, replayedHead(t, fs)} {
		require.Equal(t, []int64{0, 1}, headTS(h, valid))
		require.False(t, h.series.Has(expired.Hash()))
	}
}

package recordengine_test

import (
	"bytes"
	"testing"

	"github.com/oteldb/storage/wal"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/recordengine"
)

func TestTimestampFloorBeforeStreamAndWAL(t *testing.T) {
	t.Parallel()
	for _, primary := range []bool{false, true} {
		name := "append"
		if primary {
			name = "primary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs, w := newFaultWAL(t)
			e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w})
			limits := recordengine.AppendLimits{MinTimestamp: 0, EnforceMinTimestamp: true, MaxSeries: 1}
			expired := mkBatch("expired", rrec{ts: -1, body: "old"})
			valid := mkBatch("api", rrec{ts: -1, body: "old"}, rrec{ts: 0, body: "boundary"}, rrec{ts: 1, body: "new"})
			if primary {
				payload := append(recordengine.EncodeWAL(expired), recordengine.EncodeWAL(valid)...)
				accepted, res, err := e.ApplyPrimary(payload, limits)
				require.NoError(t, err)
				require.Equal(t, recordengine.AppendResult{Accepted: 2, RejectedRetention: 2}, res)
				replica := recordengine.New(recordengine.Config{Schema: testSchema})
				require.NoError(t, replica.ApplyReplicated(accepted))
				require.Equal(t, 1, replica.StreamCount())
				require.Equal(t, []int64{0, 1}, fetchAll(t, replica, req("api"))[0].Timestamps)
			} else {
				res, err := e.AppendBatch(expired, limits)
				require.NoError(t, err)
				require.Equal(t, 1, res.RejectedRetention)
				require.Zero(t, e.StreamCount())
				res, err = e.AppendBatch(valid, limits)
				require.NoError(t, err)
				require.Equal(t, recordengine.AppendResult{Accepted: 2, RejectedRetention: 1}, res)
			}
			require.Equal(t, 1, e.StreamCount())
			recovered := replayed(t, fs, nil)
			require.Equal(t, 1, recovered.StreamCount())
			require.Equal(t, []int64{0, 1}, fetchAll(t, recovered, req("api"))[0].Timestamps)
		})
	}
}

func TestTimestampFloorDropsRejectedSideData(t *testing.T) {
	t.Parallel()
	fs, w := newFaultWAL(t)
	side := newFakeSide()
	e := recordengine.New(recordengine.Config{Schema: testSchema, WAL: w, SideStore: side})
	b := mkBatch("expired", rrec{ts: 9, body: "old"})
	b.Side = encodeSide(map[uint64][]byte{7: []byte("symbol")})
	payload, res, err := e.ApplyPrimary(recordengine.EncodeWAL(b), recordengine.AppendLimits{MinTimestamp: 10, EnforceMinTimestamp: true})
	require.NoError(t, err)
	require.Equal(t, recordengine.AppendResult{RejectedRetention: 1}, res)
	require.Empty(t, payload)
	require.Empty(t, accIDs(side))
	require.Zero(t, side.absorbed)
	require.Zero(t, e.StreamCount())
	recoveredSide := newFakeSide()
	_ = replayed(t, fs, recoveredSide)
	require.Empty(t, accIDs(recoveredSide))
	require.Zero(t, recoveredSide.absorbed)

	var standalone bytes.Buffer
	require.NoError(t, wal.NewWriter(&standalone).WriteSide(b.Side))
	payload, res, err = e.ApplyPrimary(standalone.Bytes(), recordengine.AppendLimits{MinTimestamp: 10, EnforceMinTimestamp: true})
	require.NoError(t, err)
	require.Zero(t, res.Accepted)
	require.Zero(t, res.Rejected())
	require.NotEmpty(t, payload)
	require.Equal(t, []uint64{7}, accIDs(side))
	replicaSide := newFakeSide()
	replica := recordengine.New(recordengine.Config{Schema: testSchema, SideStore: replicaSide})
	require.NoError(t, replica.ApplyReplicated(payload))
	require.Equal(t, []uint64{7}, accIDs(replicaSide))
}

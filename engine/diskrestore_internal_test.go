package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestDiskWALRestoresAcceptedRowsBeforeHistoricalRepair(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	s, fresh := benchSeries(1), benchSeries(2)
	owner := New(Config{Backend: be, Prefix: "restore-gap", DiskSeries: true, WriterID: "primary"})
	ok, err := owner.Append(s, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, owner.Flush(t.Context()))
	dir := t.TempDir()
	writer, err := wal.Create(dir, 0)
	require.NoError(t, err)
	source := New(Config{WAL: writer})
	for _, sample := range []struct {
		s  signal.Series
		ts int64
	}{{s, 95}, {fresh, 85}} {
		ok, err := source.Append(sample.s, sample.ts, 2)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, writer.Close())
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	before, err := os.ReadFile(files[0])
	require.NoError(t, err)
	recovered := New(Config{Backend: be, Prefix: "restore-gap", DiskSeries: true, WriterID: "replica", OOOWindow: 10})
	require.NoError(t, recovered.LoadPartsUnclaimed(t.Context()))
	parts := recovered.parts
	recovered.parts = nil
	recovered.pendingWants = []bucketindex.Want{{Prefix: parts[0].prefix}}
	recovered.head.disk.complete = false
	epoch := recovered.flushedEpoch
	require.NoError(t, recovered.Replay(t.Context(), dir))
	require.EqualValues(t, 2, recovered.Stats().HeadSamples)
	require.False(t, recovered.Stats().SeriesComplete)
	require.Equal(t, epoch, recovered.flushedEpoch)
	newest, seen := recovered.head.watermark(s.Hash())
	require.True(t, seen)
	require.EqualValues(t, 95, newest)
	ok, err = recovered.Append(s, 86, 3)
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.False(t, ok)
	require.ErrorIs(t, recovered.Flush(t.Context()), bucketindex.ErrFenced, "history repair precedes publishing or checkpointing restored writes")
	after, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, epoch, recovered.flushedEpoch)
	recovered.mu.Lock()
	recovered.parts = parts
	recovered.pendingWants = nil
	err = recovered.rebuildDiskStateLocked(t.Context())
	recovered.mu.Unlock()
	require.NoError(t, err)
	require.True(t, recovered.Stats().SeriesComplete)
	require.EqualValues(t, 2, recovered.Stats().Series)
	newest, seen = recovered.head.watermark(s.Hash())
	require.True(t, seen)
	require.EqualValues(t, 100, newest, "repair restores the missing historical admission watermark")
	ok, err = recovered.Append(s, 86, 3)
	require.NoError(t, err)
	require.False(t, ok, "86 is older than repaired100 minus window10")
}

func TestDiskIncompleteHistoryFencesEveryLocalAdmission(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	e.cfg.DiskSeries = true
	e.cfg.ResidentHeadBytes = defaultResidentHeadBytes
	e.head.disk = newDiskHeadState()
	s := benchSeries(1)
	ok, err := e.Append(s, 95, 1)
	require.NoError(t, err)
	require.True(t, ok)
	e.pendingWants = []bucketindex.Want{{Prefix: "missing-history"}}
	ids, ts := []signal.SeriesID{s.Hash()}, []int64{86}
	result, err := e.AppendBatch(ids, ts, []float64{2}, nil, func(int) signal.Series { return s }, AppendLimits{})
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.Zero(t, result.Accepted)
	result, err = e.appendPrepared(ids, ts, []float64{2}, nil, func(int) signal.Series { panic("known series materialized") }, AppendLimits{}, preparedFor(ids))
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.Zero(t, result.Accepted)
	_, result, err = e.ApplyPrimary(replicaSamplePayload(t, s, replicaTestSample{86, 2, 1}), AppendLimits{})
	require.ErrorIs(t, err, bucketindex.ErrFenced)
	require.Zero(t, result.Accepted)
	// The primary's accepted payload is authoritative, even when this replica cannot classify history.
	require.NoError(t, e.ApplyReplicated(replicaSamplePayload(t, s, replicaTestSample{94, 3, 1})))
	require.Equal(t, []int64{95, 94}, e.head.samples[s.Hash()].ts)
	require.False(t, e.Stats().SeriesComplete)
}

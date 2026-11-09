package engine

import (
	"bytes"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func snapshotReplicaPressureWAL(t *testing.T, fsys *faultfs.FS) map[string][]byte {
	t.Helper()
	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)
	out := make(map[string][]byte)
	for _, entry := range entries {
		if path.Ext(entry.Name()) != ".wal" {
			continue
		}
		data, err := fsys.ReadFile(entry.Name())
		require.NoError(t, err)
		out[entry.Name()] = bytes.Clone(data)
	}
	return out
}

func TestDiskReplicaPressureRefreshesWithoutPublishing(t *testing.T) {
	t.Parallel()
	for _, publication := range []string{"none", "partial", "all"} {
		t.Run(publication, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			primaryFS, replicaFS := faultfs.New(), faultfs.New()
			primaryWAL, err := wal.CreateFS(primaryFS, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = primaryWAL.Close() })
			replicaWAL, err := wal.CreateFS(replicaFS, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = replicaWAL.Close() })
			primary := New(Config{Backend: be, Prefix: "replica-pressure", DiskSeries: true, WAL: primaryWAL, WriterID: "primary", Term: func() uint64 { return 7 }})
			replicaBackend := faultbackend.Wrap(be)
			replica := New(Config{Backend: replicaBackend, Prefix: "replica-pressure", DiskSeries: true, WAL: replicaWAL, WriterID: "replica", Term: func() uint64 { return 0 }, ResidentHeadBytes: (1 << 20) + (64 << 10)})
			old, fresh := benchSeries(1), benchSeries(2)
			write := func(s signal.Series, start int64, n int) []byte {
				ts := make([]int64, n)
				values := make([]float64, n)
				for i := range ts {
					ts[i] = start + int64(i)
					values[i] = float64(ts[i])
				}
				input := primaryPayload(t, seriesRun(s, ts, values))
				accepted, result, err := primary.ApplyPrimary(input, AppendLimits{})
				require.NoError(t, err)
				require.Equal(t, n, result.Accepted)
				return accepted
			}
			require.NoError(t, replica.ApplyReplicated(write(old, 1, 1024)))
			if publication == "partial" {
				require.NoError(t, primary.Flush(t.Context()))
			}
			require.NoError(t, replica.ApplyReplicated(write(old, 1025, 8192)))
			require.True(t, replica.residentHeadOverBudgetLocked())
			if publication == "all" {
				require.NoError(t, primary.Flush(t.Context()))
			}
			next := write(fresh, 9217, 1)
			// Pressure on the non-owner may open published parts, but it may write no object.
			for _, kind := range []faultbackend.Kind{faultbackend.Write, faultbackend.PutIfAbsent, faultbackend.CompareAndSwap, faultbackend.Delete} {
				replicaBackend.Add(faultbackend.Rule{Kind: kind, Err: errInjectedWAL})
			}
			primaryBefore := snapshotReplicaPressureWAL(t, primaryFS)
			primaryEpoch := primaryWAL.Epoch()
			replicaBefore := snapshotReplicaPressureWAL(t, replicaFS)
			replicaEpoch := replicaWAL.Epoch()
			indexBefore, readErr := be.Read(t.Context(), primary.indexKey())
			if publication == "none" {
				require.ErrorIs(t, readErr, backend.ErrNotExist)
			} else {
				require.NoError(t, readErr)
			}
			err = replica.ApplyReplicated(next)
			if publication == "all" {
				require.NoError(t, err)
				require.EqualValues(t, 1, replica.Stats().HeadSamples)
				require.EqualValues(t, 2, replica.Stats().Series)
			} else {
				require.ErrorIs(t, err, ErrReplicaHeadFull)
				expected := int64(9216)
				if publication == "partial" {
					expected = 8192
				}
				require.Equal(t, expected, replica.Stats().HeadSamples)
				require.False(t, replica.head.series.Has(fresh.Hash()), "the refused payload has not been applied")
				require.EqualValues(t, 1, replica.Stats().Series)
				require.ErrorIs(t, replica.ApplyReplicated(next), ErrReplicaHeadFull, "retries cannot grow an unpublished full replica head")
				require.Equal(t, expected, replica.Stats().HeadSamples)
			}
			require.False(t, replica.Stats().IndexFenced, "lack of ownership under pressure must not fence the replica")
			require.True(t, replica.Stats().SeriesComplete)
			indexAfter, afterErr := be.Read(t.Context(), primary.indexKey())
			if publication == "none" {
				require.ErrorIs(t, afterErr, backend.ErrNotExist)
			} else {
				require.NoError(t, afterErr)
			}
			require.Equal(t, indexBefore, indexAfter, "replica refresh cannot author the primary catalog")
			require.Equal(t, primaryEpoch, primaryWAL.Epoch())
			require.Equal(t, primaryBefore, snapshotReplicaPressureWAL(t, primaryFS), "pressure cannot checkpoint the primary WAL")
			require.Equal(t, replicaEpoch, replicaWAL.Epoch())
			require.Equal(t, replicaBefore, snapshotReplicaPressureWAL(t, replicaFS), "refresh cannot checkpoint the replica WAL")
			if publication != "all" {
				require.NoError(t, primary.Flush(t.Context()))
				require.NoError(t, replica.ApplyReplicated(next), "published primary parts release pressure on retry")
				require.EqualValues(t, 1, replica.Stats().HeadSamples)
				require.False(t, replica.Stats().IndexFenced)
			}
		})
	}
}

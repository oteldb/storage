package engine

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

type cleanupFaultBackend struct {
	*faultbackend.Backend

	source backend.Backend
}

func (b cleanupFaultBackend) ReadAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.source, key, offset, length)
}

func newCleanupFaultBackend() cleanupFaultBackend {
	source := backend.Memory()
	return cleanupFaultBackend{Backend: faultbackend.Wrap(source), source: source}
}

func TestFailedHeadBuildReleasesCompletedParts(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := separateCatalogEngine(be)
	defer func() { require.NoError(t, e.CloseWAL()) }()
	s := benchSeries(1)
	accepted, err := e.Append(s, 0, 0)
	require.NoError(t, err)
	require.True(t, accepted)
	require.NoError(t, e.Flush(t.Context()))
	before := e.seriesTable.refs.Load()
	e.cfg.MaxPartBytes = 3 * partRowBytes
	for i := range 10 {
		accepted, err = e.Append(s, int64(i+1), float64(i))
		require.NoError(t, err)
		require.True(t, accepted)
	}
	failure := errors.New("second numeric part fails")
	completed := 0
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: failure, Match: func(op faultbackend.Op) bool {
		if strings.HasSuffix(op.Key, "/sidx") {
			completed++
			return completed == 2
		}
		return false
	}})
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	require.Equal(t, 2, completed)
	require.Equal(t, before, e.seriesTable.refs.Load(), "abandoned finished part must release its snapshot")
	require.Equal(t, 10, e.HeadSampleCount())
	require.Len(t, e.parts, 1)
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
}

func TestFailedMergeBuildReleasesCompletedParts(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := separateCatalogEngine(be)
	defer func() { require.NoError(t, e.CloseWAL()) }()
	s := benchSeries(1)
	const day = int64(24 * 60 * 60 * 1e9)
	for i := range 3 {
		accepted, err := e.Append(s, int64(i+1)*day, float64(i))
		require.NoError(t, err)
		require.True(t, accepted)
	}
	require.NoError(t, e.Flush(t.Context()))
	before := e.seriesTable.refs.Load()
	failure := errors.New("second numeric output fails")
	completed := 0
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: failure, Match: func(op faultbackend.Op) bool {
		if strings.HasSuffix(op.Key, "/sidx") {
			completed++
			return completed == 2
		}
		return false
	}})
	parts, err := e.compactStream(t.Context(), e.parts, 1, rollupPlan{}, MergeOptions{})
	require.ErrorIs(t, err, failure)
	require.Nil(t, parts)
	require.Equal(t, 2, completed)
	require.Equal(t, before, e.seriesTable.refs.Load())
}

func TestFailedMergePublicationReleasesOutput(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := separateCatalogEngine(be)
	defer func() { require.NoError(t, e.CloseWAL()) }()
	s := benchSeries(1)
	for i := range 2 {
		accepted, err := e.Append(s, int64(i+1), float64(i))
		require.NoError(t, err)
		require.True(t, accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, 2)
	before := e.seriesTable.refs.Load()
	failure := errors.New("catalog publication fails")
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: failure})
	require.ErrorIs(t, e.Merge(t.Context(), 0), failure)
	require.Len(t, e.parts, 2)
	require.Equal(t, before, e.seriesTable.refs.Load())
}

func TestFailedColumnBuildReleasesCompletedParts(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := separateCatalogEngine(be)
	defer func() { require.NoError(t, e.CloseWAL()) }()
	s := benchSeries(1)
	accepted, err := e.Append(s, 1, 2)
	require.NoError(t, err)
	require.True(t, accepted)
	require.NoError(t, e.Flush(t.Context()))
	before := e.seriesTable.refs.Load()
	buf := &sampleBuf{}
	for i := range 3 {
		buf.appendSample(int64(i+2), float64(i), 1)
	}
	cols := buildFlushColumns(map[signal.SeriesID]*sampleBuf{s.Hash(): buf})
	failure := errors.New("second numeric output fails")
	completed := 0
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: failure, Match: func(op faultbackend.Op) bool {
		if strings.HasSuffix(op.Key, "/sidx") {
			completed++
			return completed == 2
		}
		return false
	}})
	parts, err := e.writeColumns(t.Context(), cols, 1, MergeOptions{}, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, parts)
	require.Equal(t, 2, completed)
	require.Equal(t, before, e.seriesTable.refs.Load())
}

func catalogWALSamples(t *testing.T, dir string, epoch uint64) map[signal.SeriesID][]int64 {
	t.Helper()
	got := make(map[signal.SeriesID][]int64)
	require.NoError(t, wal.ReplayDirFrom(dir, epoch, wal.Handlers{
		OnSamples: func(id signal.SeriesID, ts []int64, _ []float64) error {
			got[id] = append(got[id], ts...)
			return nil
		},
	}))
	return got
}

func catalogFetchTimes(t *testing.T, e *Engine) map[signal.SeriesID][]int64 {
	t.Helper()
	it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 10})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	got := make(map[signal.SeriesID][]int64)
	for {
		batch, err := it.Next(t.Context())
		if errors.Is(err, io.EOF) {
			return got
		}
		require.NoError(t, err)
		got[batch.ID] = append(got[batch.ID], batch.Timestamps...)
	}
}

func TestFlushCatalogFailureKeepsWALUntilPublished(t *testing.T) {
	t.Parallel()
	for _, recovery := range []string{"retry", "restart"} {
		t.Run(recovery, func(t *testing.T) {
			t.Parallel()
			be := newCleanupFaultBackend()
			dir := t.TempDir()
			journal, err := wal.Create(dir, 0)
			require.NoError(t, err)
			cfg := Config{Backend: be, Prefix: "series-catalog", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true, WAL: journal}
			e := New(cfg)
			defer func() { require.NoError(t, e.CloseWAL()) }()
			first, second := benchSeries(1), benchSeries(2)
			_, err = e.Append(first, 1, 1)
			require.NoError(t, err)
			require.NoError(t, e.Flush(t.Context()))
			before, err := bucketindex.Load(t.Context(), be, e.indexKey())
			require.NoError(t, err)
			_, err = e.Append(second, 2, 2)
			require.NoError(t, err)
			failure := errors.New("flush catalog publication fails")
			be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: failure})
			require.ErrorIs(t, e.Flush(t.Context()), failure)
			require.Len(t, e.parts, 2)
			require.Len(t, e.seriesTable.bindings(), 2)
			require.NotNil(t, e.parts[1].pending)
			after, err := bucketindex.Load(t.Context(), be, e.indexKey())
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, map[signal.SeriesID][]int64{second.Hash(): {2}}, catalogWALSamples(t, dir, before.FlushedEpoch))
			want := map[signal.SeriesID][]int64{first.Hash(): {1}, second.Hash(): {2}}
			require.Equal(t, want, catalogFetchTimes(t, e), "retained output remains queryable after failed CAS")
			be.Reset()
			if recovery == "retry" {
				_, err = e.Append(second, 3, 3)
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
				want[second.Hash()] = []int64{2, 3}
			}
			require.NoError(t, e.CloseWAL())
			cfg.WAL, err = wal.Create(dir, 0)
			require.NoError(t, err)
			recovered := New(cfg)
			defer func() { require.NoError(t, recovered.CloseWAL()) }()
			require.NoError(t, recovered.LoadParts(t.Context()))
			require.NoError(t, recovered.Replay(t.Context(), dir))
			require.Equal(t, want, catalogFetchTimes(t, recovered))
			if recovery == "retry" {
				require.Zero(t, recovered.HeadSampleCount(), "confirmed catalog checkpoint must prevent replay duplicates")
			} else {
				require.Equal(t, 1, recovered.HeadSampleCount(), "unpublished samples must replay")
			}
			require.NoError(t, recovered.Flush(t.Context()))
			committed, err := bucketindex.Load(t.Context(), be, recovered.indexKey())
			require.NoError(t, err)
			require.Len(t, committed.SeriesRoots, 2)
			require.Greater(t, committed.FlushedEpoch, before.FlushedEpoch)
			require.Empty(t, catalogWALSamples(t, dir, committed.FlushedEpoch))
			require.Equal(t, want, catalogFetchTimes(t, recovered))
		})
	}
}

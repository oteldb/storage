package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestReturningIDHeadReadsAndReplays(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e, fs := newReturningIDEngine(t, files)
	point := func(value string) signal.KeyValue {
		return signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(value))}
	}
	s := signal.Series{
		Resource:   signal.Resource{Attributes: signal.NewAttributes(point("resource"))},
		Attributes: signal.NewAttributes(point("point")),
	}
	_, err = e.Append(s, 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))

	id := s.Hash()
	r, err := e.AppendBatch([]signal.SeriesID{id, id}, []int64{200, 201}, []float64{2, 3}, nil,
		func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 2, r.Accepted)
	require.Len(t, e.head.encodedRefs, 1)
	require.Equal(t, s.AppendHashInput(nil), e.head.series.View().EncodedDescriptor(int(e.head.encodedRefs[0])))
	require.Zero(t, e.head.sym.Len(), "returning attachments skip eager label interning")
	cfg := e.cfg
	cfg.WAL = nil
	reopened := New(cfg)
	require.NoError(t, reopened.LoadPartsReadOnly(t.Context()))
	require.True(t, reopened.compactMetricMetadata)
	entries, err := fs.ReadDir(".")
	require.NoError(t, err)
	require.Len(t, entries, 1, "the descriptor WAL was checkpointed")
	data, err := fs.ReadFile(entries[0].Name())
	require.NoError(t, err)
	var descriptors, references int
	require.NoError(t, wal.Replay(data, wal.Handlers{
		OnSeries: func(signal.SeriesID, signal.Series) error { descriptors++; return nil },
		OnDescriptorReference: func(got signal.SeriesID) error {
			require.Equal(t, id, got)
			references++
			return nil
		},
	}))
	require.Zero(t, descriptors)
	require.Equal(t, 1, references)
	require.ErrorIs(t, wal.Replay(data, New(Config{}).replayHandlers()), wal.ErrCorrupt)
	require.NoError(t, wal.Replay(data, reopened.salvageHandlers(t.Context(), "")))
	require.Equal(t, []int64{200, 201}, headTS(reopened.head, s))
	require.NoError(t, reopened.CloseWAL())

	for _, value := range []string{"resource", "point", "missing"} {
		matcher := fetch.EqualMatcher{Name: "host", Value: value}
		req := fetch.Request{Start: 200, End: 201, Matchers: []fetch.Matcher{
			{Name: []byte(matcher.Name), Match: matcher.Predicate(), Spec: &matcher},
		}}
		count, err := e.Count(t.Context(), req)
		require.NoError(t, err)
		it, err := e.Fetch(t.Context(), req)
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		if value == "missing" {
			require.Zero(t, count)
			require.Empty(t, batches)
			continue
		}
		require.Equal(t, 1, count)
		require.Len(t, batches, 1)
		require.Equal(t, []int64{200, 201}, batches[0].Timestamps)
	}
	require.Equal(t, 1, e.Cardinality(0).DistinctLabelNames)
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.LoadParts(t.Context()))
	require.NoError(t, e.Close(t.Context()))
}

func newReturningIDEngine(t *testing.T, b backend.Backend) (*Engine, *faultfs.FS) {
	t.Helper()
	e, fs := newFaultWALEngine(t, 10)
	e.cfg.Backend, e.cfg.Prefix = b, "returning"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.SeparateSeries, e.cfg.MetricSeriesChunks = true, true
	e.cfg.ResidentHeadBytes, e.cfg.HeadRotationBytes, e.cfg.FlushMemoryBytes = 1<<30, 256<<20, 64<<20
	pool := memlimit.NewPool(512 << 20)
	e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
	e.head.disk = newDiskHeadState()
	return e, fs
}

func TestWALDescriptorReferenceRequiresSuccessfulCommit(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	gate := &generationGateBackend{Backend: files}
	e, fs := newReturningIDEngine(t, gate)

	_, err = e.Append(stagedTestSeries("published"), 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	require.True(t, e.compactMetricMetadata)

	fresh := stagedTestSeries("unpublished")
	id := fresh.Hash()
	_, err = e.Append(fresh, 200, 2)
	require.NoError(t, err)
	gate.commitFail.Store(true)
	require.ErrorIs(t, e.Flush(t.Context()), context.DeadlineExceeded)
	entry, present := e.head.disk.cache.get(id)
	require.True(t, present)
	require.False(t, entry.descriptorPublished)

	res, err := e.AppendBatch([]signal.SeriesID{id}, []int64{201}, []float64{3}, nil,
		func(int) signal.Series { return fresh }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 1, res.Accepted)
	require.Equal(t, []int64{200, 201}, headTS(replayedHead(t, fs), fresh))
}

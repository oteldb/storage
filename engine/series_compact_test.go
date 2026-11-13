package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

func compactSeriesFixture(t *testing.T, be backend.Backend) *Engine {
	t.Helper()
	e := separateCatalogEngine(be)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	for i := range 8 {
		ok, err := e.Append(benchSeries(i), int64(i+1), float64(i))
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.seriesTable.roots, 8)
	return e
}

func TestSeriesCompactionPinsAndRecovery(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := compactSeriesFixture(t, be)
	previous := e.parts[len(e.parts)-1].sharedSeries.acquire()
	defer previous.release()
	before := catalogFetchTimes(t, e)
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, e.seriesTable.roots, 1)
	require.Len(t, e.seriesRetired, 8)
	require.Equal(t, before, catalogFetchTimes(t, e))
	for _, p := range e.parts {
		held := p.sharedSeries.acquire()
		require.Len(t, held.roots, 1)
		held.release()
	}
	require.Len(t, previous.roots, 8)
	for i := range 8 {
		found, err := previous.lookup(t.Context(), benchSeries(i).Hash(), func([]byte) error { return nil })
		require.NoError(t, err)
		require.True(t, found)
	}
	require.NoError(t, e.Close(t.Context()))
	recovered := separateCatalogEngine(be)
	require.NoError(t, recovered.LoadParts(t.Context()))
	defer func() { require.NoError(t, recovered.Close(t.Context())) }()
	require.Len(t, recovered.seriesTable.roots, 1)
	require.Len(t, recovered.seriesRetired, 8)
	require.Equal(t, before, catalogFetchTimes(t, recovered))
}

func TestSeriesCompactionCommitFailureKeepsPublishedBindings(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := compactSeriesFixture(t, be)
	before, err := be.Read(t.Context(), e.indexKey())
	require.NoError(t, err)
	binding := e.parts[len(e.parts)-1].sharedSeries.acquire()
	defer binding.release()
	failure := errors.New("metadata CAS failed")
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: failure})
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.ErrorIs(t, err, failure)
	require.False(t, changed)
	after, err := be.Read(t.Context(), e.indexKey())
	require.NoError(t, err)
	require.Equal(t, before, after)
	held := e.parts[len(e.parts)-1].sharedSeries.acquire()
	require.Same(t, binding, held)
	held.release()
	require.Len(t, e.seriesTable.roots, 1, "synced output remains pending for the next CAS")
	be.Reset()
	e.mu.Lock()
	err = e.updateIndexLocked(t.Context())
	e.mu.Unlock()
	require.NoError(t, err)
	held = e.parts[len(e.parts)-1].sharedSeries.acquire()
	require.Len(t, held.roots, 1)
	held.release()
}

func TestSeriesCompactionStaleRebaseDoesNotResurrectRoots(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := compactSeriesFixture(t, be)
	stale := separateCatalogEngine(be)
	require.NoError(t, stale.LoadPartsReadOnly(t.Context()))
	defer func() { require.NoError(t, stale.CloseWAL()) }()
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	stale.mu.Lock()
	err = stale.updateIndexLocked(t.Context())
	stale.mu.Unlock()
	require.NoError(t, err)
	ix, err := bucketindex.Load(t.Context(), be, e.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.SeriesRoots, 1)
	require.Len(t, ix.RetiredSeriesRoots, 8)
	require.Len(t, stale.seriesTable.roots, 1)
}

func TestSeriesCompactionCancelled(t *testing.T) {
	t.Parallel()
	e := compactSeriesFixture(t, backend.Memory())
	before := e.seriesTable
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	changed, err := e.compactSeriesRuns(ctx, false)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, changed)
	require.Same(t, before, e.seriesTable)
}

func TestSeriesCompactionBoundedAdmission(t *testing.T) {
	t.Parallel()
	e := compactSeriesFixture(t, backend.Memory())
	e.cfg.MergeMemoryBytes = 16 << 20
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.False(t, changed)
	require.Len(t, e.seriesTable.roots, 8)
}

func TestSeriesCompactionDuplicateIdentities(t *testing.T) {
	t.Parallel()
	e := compactSeriesFixture(t, backend.Memory())
	// Distinct validated runs may repeat identities; their union must preserve one descriptor.
	sources := []*seriesRootHandle{e.seriesTable.roots[0], e.seriesTable.roots[0]}
	out, err := e.buildSeriesMerge(t.Context(), sources, 64<<20)
	require.NoError(t, err)
	defer func() {
		for _, h := range out {
			h.release()
		}
	}()
	require.Len(t, out, 1)
	require.EqualValues(t, 1, out[0].run.Count())
	var ids []signal.SeriesID
	it, err := out[0].run.Iterate(t.Context())
	require.NoError(t, err)
	for it.Next() {
		ids = append(ids, it.ID())
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Len(t, ids, 1)
	reader, err := out[0].run.NewReader(t.Context())
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestSeriesCompactionConcurrentAppend(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := compactSeriesFixture(t, be)
	entered, release := make(chan struct{}), make(chan struct{})
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Before: func(faultbackend.Op) { close(entered); <-release }})
	done := make(chan error, 1)
	go func() { _, err := e.compactSeriesRuns(t.Context(), false); done <- err }()
	<-entered
	ok, err := e.Append(benchSeries(100), 100, 100)
	require.NoError(t, err)
	require.True(t, ok)
	close(release)
	require.NoError(t, <-done)
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
	ix, err := bucketindex.Load(t.Context(), be, e.indexKey())
	require.NoError(t, err)
	require.Len(t, ix.SeriesRoots, 2, "append admission remains independent and its new identity is published")
	require.Len(t, ix.RetiredSeriesRoots, 8)
}

func TestSeriesCompactionEstimatePinsSnapshot(t *testing.T) {
	t.Parallel()
	e := compactSeriesFixture(t, backend.Memory())
	p := e.parts[len(e.parts)-1]
	ctx, release := withPartSeriesSnapshot(t.Context(), p)
	held := p.acquireSeriesSnapshot(ctx)
	source := held.roots[0].run
	held.release()
	before, err := p.diskSeriesWorkspace(ctx, 2)
	require.NoError(t, err)
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	after, err := p.diskSeriesWorkspace(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, before, after)
	held = p.acquireSeriesSnapshot(ctx)
	require.Len(t, held.roots, 8, "planning and descriptor access share one immutable snapshot")
	held.release()
	release()
	_, err = source.NewReader(t.Context())
	require.ErrorIs(t, err, seriestable.ErrClosed, "old run headers retire after the last operation lease")
}

func TestSeriesCompactionReusedLoadRebinds(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := compactSeriesFixture(t, be)
	stale := separateCatalogEngine(be)
	require.NoError(t, stale.LoadPartsReadOnly(t.Context()))
	defer func() { require.NoError(t, stale.CloseWAL()) }()
	first := stale.parts[0]
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	require.NoError(t, stale.LoadPartsReadOnly(t.Context()))
	require.Same(t, first, stale.parts[0], "the immutable numeric handle is reused")
	for _, p := range stale.parts {
		held := p.sharedSeries.acquire()
		require.Len(t, held.roots, 1)
		held.release()
	}
}

func TestSeriesCompactionLoadPreservesUncommittedPartBinding(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := compactSeriesFixture(t, be)
	failure := errors.New("new identity flush CAS failed")
	accepted, err := e.Append(benchSeries(100), 100, 100)
	require.NoError(t, err)
	require.True(t, accepted)
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: failure})
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	be.Reset()
	require.NoError(t, e.LoadPartsReadOnly(t.Context()))
	found := false
	for _, p := range e.parts {
		if p.pending == nil {
			continue
		}
		ok, err := p.diskSeriesLookupRaw(t.Context(), benchSeries(100).Hash(), func([]byte) error { return nil })
		require.NoError(t, err)
		require.True(t, ok)
		found = true
	}
	require.True(t, found)
}

type directSeriesReadFile struct{ *backendfile.File }

func (f directSeriesReadFile) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.File.ReadAt(context.Background(), key, off, n) //nolint:contextcheck // Force physical reads without retained handles.
}

func (f directSeriesReadFile) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.File.ReadAtAppend(context.Background(), dst, key, off, n) //nolint:contextcheck // Force physical reads without retained handles.
}

func (directSeriesReadFile) RetainsImmutableReaders() bool { return false }

func TestSeriesCompactionSweepKeepsRetiredSources(t *testing.T) {
	t.Parallel()
	for _, files := range []bool{false, true} {
		t.Run(fmt.Sprintf("file=%t", files), func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			if files {
				f, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				be = directSeriesReadFile{f}
			}
			e := compactSeriesFixture(t, be)
			previous := e.seriesTable
			previous.acquire()
			defer previous.release()
			changed, err := e.compactSeriesRuns(t.Context(), false)
			require.NoError(t, err)
			require.True(t, changed)
			e.cfg.Now = func() time.Time { return time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC) }
			require.NoError(t, e.LoadParts(t.Context()))
			for i := range 8 {
				_, err := be.Read(t.Context(), previous.roots[i].binding.Prefix+"/series-index")
				require.NoError(t, err, "retired source bytes survive settled orphan sweeps without an open-FD shortcut")
				found, err := previous.lookup(t.Context(), benchSeries(i).Hash(), func([]byte) error { return nil })
				require.NoError(t, err)
				require.True(t, found)
			}
		})
	}
}

func TestSeriesCompactionShrinksOversizedSourceGroup(t *testing.T) {
	t.Parallel()
	e := separateCatalogEngine(backend.Memory())
	defer func() { require.NoError(t, e.CloseWAL()) }()
	for i := range 8 {
		s := benchSeries(i)
		s.Attributes = signal.NewAttributes(append(s.Attributes, signal.KeyValue{Key: []byte("blob"), Value: signal.StringValue(bytes.Repeat([]byte("x"), 30000))})...)
		accepted, err := e.Append(s, int64(i+1), 1)
		require.NoError(t, err)
		require.True(t, accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	e.cfg.MergeMemoryBytes = 64 << 20
	sources, _ := boundedSeriesSources(e.seriesTable.roots, e.mergeMemoryBudgetBytes())
	require.GreaterOrEqual(t, len(sources), 2)
	require.Less(t, len(sources), 8)
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed, "a smaller productive group fits the same memory allowance")
	require.Less(t, len(e.seriesTable.roots), 8)
}

func TestSeriesCompactionCleansCancelledPrivateOutput(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := compactSeriesFixture(t, be)
	before, err := be.List(t.Context(), e.cfg.Prefix+"/")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Match: func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, "/series-index") }, Before: func(faultbackend.Op) { cancel() }})
	changed, err := e.compactSeriesRuns(ctx, false)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, changed)
	after, err := be.List(t.Context(), e.cfg.Prefix+"/")
	require.NoError(t, err)
	require.ElementsMatch(t, before, after, "private output cleanup uses an independent bounded context")
	require.Len(t, e.seriesTable.roots, 8)
}

package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/internal/vfs/faultfs"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

type generationGateBackend struct {
	backend.Backend

	first, resume chan struct{}
	secondResume  chan struct{}
	identities    atomic.Int32
	markerSuffix  string
	firstErr      error
	commitFail    atomic.Bool
}

func (b *generationGateBackend) Write(ctx context.Context, key string, data []byte) error {
	suffix := b.markerSuffix
	if suffix == "" {
		suffix = "/identity"
	}
	if strings.HasSuffix(key, suffix) {
		n := b.identities.Add(1)
		if n == 2 && b.secondResume != nil {
			select {
			case <-b.secondResume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if n != 1 {
			return b.Backend.Write(ctx, key, data)
		}
		close(b.first)
		select {
		case <-b.resume:
			if b.firstErr != nil {
				return b.firstErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Backend.Write(ctx, key, data)
}

func TestSharedSeriesGenerationsBuildAheadWithoutCheckpointGap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, _, resume, job := blockedGenerationPair(t, nil, func(e *Engine, b *generationGateBackend) {
			e.cfg.SeparateSeries, e.cfg.MetricSeriesChunks = true, true
			b.markerSuffix = "/series-ref"
		})
		require.True(t, e.pressureGenerations[1].ready)
		require.Len(t, e.seriesTable.roots, 1, "a later generation must reuse the earlier descriptors")
		require.Zero(t, e.flushedEpoch)
		for _, buf := range replayedHead(t, fs).samples {
			require.Equal(t, []int64{100, 200}, buf.ts)
		}
		resume()
		synctest.Wait()
		require.NoError(t, job.err)
		require.NoError(t, e.LoadParts(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 200})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, batches, 64)
		for _, batch := range batches {
			require.Equal(t, []int64{100, 200}, batch.Timestamps)
			require.Equal(t, []float64{1, 2}, batch.Values)
		}
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsBuildAheadWithoutCheckpointGap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, be, resume, _ := blockedGenerationPair(t, nil)
		require.EqualValues(t, 2, be.identities.Load(), "second generation must finish its output while the first is blocked")
		require.Zero(t, e.flushedEpoch, "later output cannot advance the WAL coverage frontier")
		assertGenerationCrashReplay(t, e, fs)
		recovered := replayedHead(t, fs)
		for _, buf := range recovered.samples {
			require.Equal(t, []int64{100, 200}, buf.ts)
		}
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		rows, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, rows, 64)
		resume()
		synctest.Wait()
		require.NoError(t, e.Flush(t.Context()))
		require.NoError(t, e.Close(t.Context()))
	})
}

func blockedGenerationPair(
	t *testing.T, firstErr error, configure ...func(*Engine, *generationGateBackend),
) (*Engine, *faultfs.FS, *generationGateBackend, func(), *pressureJob) {
	t.Helper()
	e, fs := newFaultWALEngine(t, 10)
	be := &generationGateBackend{Backend: backend.Memory(), first: make(chan struct{}), resume: make(chan struct{}), firstErr: firstErr}
	for _, apply := range configure {
		apply(e, be)
	}
	resume := sync.OnceFunc(func() { close(be.resume) })
	t.Cleanup(resume)
	e.cfg.Backend, e.cfg.Prefix = be, "generations"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush = true, true
	e.cfg.PressureGenerations = 2
	e.cfg.ResidentHeadBytes = 1 << 30
	e.head.disk = newDiskHeadState()
	for i := range 64 {
		_, err := e.Append(benchSeries(i), 100, 1)
		require.NoError(t, err)
	}
	target := 2 * (e.head.residentIdentityBytes() + e.head.inFlightBytes())
	e.cfg.ResidentHeadBytes = target
	require.NoError(t, e.flushResidentHead())
	<-be.first
	synctest.Wait()
	e.cfg.ResidentHeadBytes = 1 << 30
	for i := range 64 {
		_, err := e.Append(benchSeries(i), 200, 2)
		require.NoError(t, err)
	}
	e.cfg.ResidentHeadBytes = target
	job := e.startPressureJob()
	synctest.Wait()
	require.EqualValues(t, 2, be.identities.Load())
	return e, fs, be, resume, job
}

func TestPressureGenerationsRetryKeepsCompletedOutput(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, be, resume, job := blockedGenerationPair(t, context.DeadlineExceeded)
		first, second := e.pressureGenerations[0], e.pressureGenerations[1]
		require.True(t, second.ready)
		prefix := second.parts[0].prefix
		owner := first.head
		slots := e.pressureGenerations[:cap(e.pressureGenerations)]
		snapshot, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		resume()
		synctest.Wait()
		require.ErrorIs(t, job.err, context.DeadlineExceeded)
		require.Len(t, e.pressureGenerations, 2)
		require.Same(t, owner, e.pressureGenerations[0].head, "failed generation remains immutable in place")
		require.True(t, e.pressureGenerations[1].ready)
		require.Zero(t, e.flushedEpoch)
		require.Len(t, replayedHead(t, fs).samples, 64)
		_, err = e.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
		require.EqualValues(t, 3, be.identities.Load(), "only failed first output is rebuilt")
		require.Equal(t, prefix, e.parts[1].prefix)
		require.Empty(t, e.pressureGenerations)
		require.Nil(t, first.head)
		require.Nil(t, owner.samples)
		for _, retired := range slots {
			require.Nil(t, retired)
		}
		require.Nil(t, e.head.frozen)
		require.Zero(t, e.head.detachedBytes)
		batches, err := fetch.Drain(t.Context(), snapshot)
		require.NoError(t, err)
		assertGenerationValues(t, batches, []int64{100, 200}, []float64{1, 2})
		reopened := New(Config{Backend: be.Backend, Prefix: e.cfg.Prefix, DiskSeries: true})
		require.NoError(t, reopened.LoadParts(t.Context()))
		it, err := reopened.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		batches, err = fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		assertGenerationValues(t, batches, []int64{100, 200}, []float64{1, 2})
		require.NoError(t, reopened.CloseWAL())
		require.NoError(t, e.Close(t.Context()))
	})
}

func assertGenerationValues(t *testing.T, batches []*fetch.Batch, ts []int64, values []float64) {
	t.Helper()
	require.Len(t, batches, 64)
	for _, b := range batches {
		require.Equal(t, ts, b.Timestamps)
		require.Equal(t, values, b.Values)
	}
}

func TestPressureGenerationsBoundedAndAllReadPaths(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _, _, resume, _ := blockedGenerationPair(t, nil)
		target := e.cfg.ResidentHeadBytes
		e.cfg.ResidentHeadBytes = 1 << 30
		for i := range 64 {
			_, err := e.Append(benchSeries(i), 300, 3)
			require.NoError(t, err)
		}
		e.cfg.ResidentHeadBytes = target
		e.schedulePressureFlush()
		synctest.Wait()
		require.Len(t, e.pressureGenerations, 2)
		require.Equal(t, 64, e.HeadSampleCount())
		require.Positive(t, e.pressureGenerations[1].head.buildRetained)
		for _, ts := range []int64{100, 200, 300} {
			req := fetch.Request{Start: ts, End: ts}
			n, err := e.Count(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, 64, n)
			names, err := e.LabelNames(t.Context(), req)
			require.NoError(t, err)
			require.NotEmpty(t, names)
			grid, err := e.CountGrid(t.Context(), req, fetch.WindowGrid{Start: ts, Step: 1, Width: 1, NumSteps: 1}, nil)
			require.NoError(t, err)
			require.Equal(t, []int{64}, grid[""])
		}
		require.EqualValues(t, 64, e.Cardinality(0).TotalSeries)
		done := make(chan error, 1)
		go func() { _, err := e.Append(benchSeries(0), 400, 4); done <- err }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("full aggregate budget must block a third frozen generation")
		default:
		}
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsCloseWALRetainsReplay(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, _, _, _ := blockedGenerationPair(t, nil)
		done := make(chan error, 1)
		go func() { done <- e.CloseWAL() }()
		synctest.Wait()
		require.NoError(t, <-done)
		require.Zero(t, e.flushedEpoch)
		for _, buf := range replayedHead(t, fs).samples {
			require.Equal(t, []int64{100, 200}, buf.ts)
		}
		for _, g := range e.pressureGenerations {
			require.False(t, g.building)
			require.Empty(t, g.parts)
			require.Zero(t, g.head.buildRetained)
		}
	})
}

func (b *generationGateBackend) CompareAndSwap(ctx context.Context, key string, expected backend.Version, data []byte) (backend.Version, bool, error) {
	if b.commitFail.Load() {
		return backend.VersionAbsent, false, context.DeadlineExceeded
	}
	return b.Backend.CompareAndSwap(ctx, key, expected, data)
}

func TestPressureGenerationsCatalogFailureRetainsCandidates(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, be, resume, job := blockedGenerationPair(t, nil)
		secondPrefix := e.pressureGenerations[1].parts[0].prefix
		firstOwner := e.pressureGenerations[0].head
		be.commitFail.Store(true)
		resume()
		synctest.Wait()
		require.ErrorIs(t, job.err, context.DeadlineExceeded)
		require.Len(t, e.parts, 1)
		require.Len(t, e.pressureGenerations, 1)
		require.Nil(t, firstOwner.memory.numeric, "local candidate installation releases input even without catalog confirmation")
		require.Len(t, replayedHead(t, fs).samples, 64, "unconfirmed catalog must retain WAL")
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		assertGenerationValues(t, batches, []int64{100, 200}, []float64{1, 2})
		be.commitFail.Store(false)
		require.NoError(t, e.Flush(t.Context()))
		require.EqualValues(t, 2, be.identities.Load(), "completed candidates are reused on retry")
		require.Equal(t, secondPrefix, e.parts[1].prefix)
		require.Empty(t, replayedHead(t, fs).samples)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsDuplicatePrecedenceIgnoresBuildOrder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "inverted"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		entered, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		defer release()
		var firstEpoch uint64
		e.pressure.beforeBuild = func(epoch uint64) {
			if epoch == firstEpoch {
				close(entered)
				<-resume
			}
		}
		for i := range 64 {
			_, err := e.Append(benchSeries(i), 100, 1)
			require.NoError(t, err)
		}
		firstEpoch = e.cfg.WAL.Epoch()
		target := 2 * (e.head.residentIdentityBytes() + e.head.inFlightBytes())
		e.cfg.ResidentHeadBytes = target
		require.NoError(t, e.flushResidentHead())
		<-entered
		synctest.Wait()
		e.cfg.ResidentHeadBytes = 1 << 30
		for i := range 64 {
			_, err := e.Append(benchSeries(i), 100, 2)
			require.NoError(t, err)
		}
		e.cfg.ResidentHeadBytes = target
		e.schedulePressureFlush()
		synctest.Wait()
		require.Len(t, e.pressureGenerations, 2)
		require.Empty(t, e.pressureGenerations[0].parts)
		require.True(t, e.pressureGenerations[1].ready, "later generation builds before the first creates any part")
		secondPrefix := e.pressureGenerations[1].parts[0].prefix
		require.NoError(t, e.LoadParts(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 200})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		assertGenerationValues(t, batches, []int64{100}, []float64{2})
		release()
		synctest.Wait()
		require.NoError(t, e.Flush(t.Context()))
		require.Equal(t, secondPrefix, e.parts[1].prefix)
		for _, merge := range []bool{false, true} {
			if merge {
				require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
			}
			reopened := New(Config{Backend: e.cfg.Backend, Prefix: e.cfg.Prefix, DiskSeries: true})
			require.NoError(t, reopened.LoadParts(t.Context()))
			it, err := reopened.Fetch(t.Context(), fetch.Request{Start: 0, End: 200})
			require.NoError(t, err)
			batches, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			assertGenerationValues(t, batches, []int64{100}, []float64{2})
			require.NoError(t, reopened.CloseWAL())
		}
		require.NoError(t, e.Close(t.Context()))
	})
}

func (b *generationGateBackend) ReadAt(ctx context.Context, key string, offset, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, offset, n)
}
func (b *generationGateBackend) NativeSize(ctx context.Context, key string) (int64, bool, error) {
	return backend.NativeSizeOf(ctx, b.Backend, key)
}

func assertGenerationCrashReplay(t *testing.T, e *Engine, fs *faultfs.FS) {
	t.Helper()
	dir := t.TempDir()
	entries, err := fs.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := fs.ReadFile(entry.Name())
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, entry.Name()), data, 0o600))
	}
	recovered := New(Config{Backend: e.cfg.Backend, Prefix: e.cfg.Prefix, DiskSeries: true, ResidentHeadBytes: 1 << 30})
	require.NoError(t, recovered.LoadParts(t.Context()))
	require.NoError(t, recovered.Replay(t.Context(), dir))
	it, err := recovered.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
	require.NoError(t, err)
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	assertGenerationValues(t, batches, []int64{100, 200}, []float64{1, 2})
	require.NoError(t, recovered.CloseWAL())
}

func TestPressureGenerationsCheckpointFailureReplay(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, _, resume, job := blockedGenerationPair(t, nil)
		fs.Add(faultfs.Rule{Op: faultfs.OpRemove, Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }, Err: context.DeadlineExceeded, Times: 1})
		resume()
		synctest.Wait()
		require.ErrorIs(t, job.err, context.DeadlineExceeded)
		require.Len(t, e.parts, 1)
		require.Len(t, e.pressureGenerations, 1)
		assertGenerationCrashReplay(t, e, fs)
		require.NoError(t, e.Flush(t.Context()))
		require.Empty(t, replayedHead(t, fs).samples)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsFenceAndReload(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs, be, resume, job := blockedGenerationPair(t, nil)
		e.mu.Lock()
		e.loadErr = context.DeadlineExceeded
		e.mu.Unlock()
		resume()
		synctest.Wait()
		require.Error(t, job.err)
		require.Empty(t, e.parts)
		require.Len(t, e.pressureGenerations, 2)
		require.Zero(t, e.flushedEpoch)
		require.Len(t, replayedHead(t, fs).samples, 64)
		require.NoError(t, e.LoadParts(t.Context()))
		require.NoError(t, e.Flush(t.Context()))
		require.EqualValues(t, 2, be.identities.Load(), "reload keeps completed fenced candidates for retry")
		require.Empty(t, e.pressureGenerations)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsResetDrainsOwners(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _, _, resume, _ := blockedGenerationPair(t, nil)
		available := e.flushMu.TryLock()
		if available {
			e.flushMu.Unlock()
		}
		require.False(t, available, "reset's exclusive lifecycle lease cannot pass a live generation")
		done := make(chan error, 1)
		go func() { done <- e.Reset(t.Context()) }()
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.Empty(t, e.pressureGenerations)
		require.Nil(t, e.head.frozen)
		require.Nil(t, e.flushing)
		require.Nil(t, e.flushingNext)
		require.Zero(t, e.PartCount())
		require.Zero(t, e.HeadBytes())
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsWakeBeforeLastBuilderCompletes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		secondResume := make(chan struct{})
		finishSecond := sync.OnceFunc(func() { close(secondResume) })
		defer finishSecond()
		e, _, _, resume, job := blockedGenerationPair(t, nil, func(_ *Engine, be *generationGateBackend) { be.secondResume = secondResume })
		target := e.cfg.ResidentHeadBytes
		e.cfg.ResidentHeadBytes = 1 << 30
		_, err := e.Append(benchSeries(0), 300, 3)
		require.NoError(t, err)
		e.cfg.ResidentHeadBytes = target
		done := make(chan error, 1)
		go func() { _, err := e.Append(benchSeries(1), 300, 3); done <- err }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("writer must wait while all aggregate credits are held")
		default:
		}
		resume()
		synctest.Wait()
		select {
		case err := <-done:
			require.NoError(t, err)
		default:
			finishSecond()
			t.Fatal("retired first generation must wake the writer before the second completes")
		}
		select {
		case <-job.done:
			t.Fatal("the second generation remains blocked")
		default:
		}
		finishSecond()
		synctest.Wait()
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureGenerationsObserveEachPublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		observability, metrics := obstest.New(t)
		e, _, _, resume, _ := blockedGenerationPair(t, nil, func(e *Engine, _ *generationGateBackend) { e.cfg.Obs = observability })
		require.Zero(t, metrics.Counter("storage.flush.total"))
		resume()
		synctest.Wait()
		require.EqualValues(t, 2, metrics.Counter("storage.flush.total"), "both generations count once, not once per supervisor pass")
		require.NoError(t, e.Close(t.Context()))
	})
}

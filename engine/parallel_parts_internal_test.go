package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type parallelPartGate struct {
	*backendfile.File

	mu                     sync.Mutex
	first                  string
	seen                   map[string]struct{}
	failOther              bool
	entered, other, resume chan struct{}
	otherOnce              sync.Once
}

func (b *parallelPartGate) CreateObject(ctx context.Context, key string) (backend.ObjectWriter, error) {
	if err := b.beforeObject(ctx, key); err != nil {
		return nil, err
	}
	return b.File.CreateObject(ctx, key)
}

func (b *parallelPartGate) CreateObjectDeferred(ctx context.Context, key string) (backend.ObjectWriter, error) {
	if err := b.beforeObject(ctx, key); err != nil {
		return nil, err
	}
	return b.File.CreateObjectDeferred(ctx, key)
}

func (b *parallelPartGate) beforeObject(ctx context.Context, key string) error {
	prefix, _, column := strings.Cut(key, "/c/")
	if !column {
		return nil
	}
	b.mu.Lock()
	if b.seen == nil {
		b.seen = make(map[string]struct{})
	}
	b.seen[prefix] = struct{}{}
	if b.first == "" {
		b.first = prefix
		close(b.entered)
	}
	first := b.first == prefix
	b.mu.Unlock()
	if first {
		select {
		case <-b.resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		b.otherOnce.Do(func() { close(b.other) })
		if b.failOther {
			return errParallelPartWrite
		}
	}
	return nil
}

var errParallelPartWrite = errors.New("injected parallel part write failure")

func TestParallelPartBuildsReachWritersBeforePublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const share = 32 << 20
		pool := memlimit.NewPool(4 * share)
		merge, err := pool.Acquire(t.Context(), 3*share)
		require.NoError(t, err)
		defer merge()
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		be := &parallelPartGate{File: file, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e, fs := newFaultWALEngine(t, 10)
		observability, measured := obstest.New(t)
		e.cfg.Obs = observability
		e.cfg.Backend, e.cfg.Prefix = be, "parallel-parts"
		e.cfg.DiskSeries, e.cfg.MetricSeriesChunks = true, true
		e.cfg.ResidentHeadBytes, e.cfg.FlushMemoryBytes = 1<<30, share
		e.cfg.MaxPartBytes = 4 * partRowBytes
		e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
		e.head.disk = newDiskHeadState()
		for i := range 16 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-be.entered
		synctest.Wait()
		overlapped := false
		select {
		case <-be.other:
			overlapped = true
		default:
		}
		require.Zero(t, e.PartCount(), "no output publishes before the entire frozen stage completes")
		_, err = e.Append(benchSeries(0), 200, 2)
		require.NoError(t, err)
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, batches, 16)
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.True(t, overlapped, "another planned part must reach its object writer while the first remains blocked")
		require.Equal(t, 4, e.PartCount(), "concurrency must not split the existing four-row parts")
		require.Len(t, be.seen, 4, "parallel work must not produce abandoned retry parts")
		require.Zero(t, measured.Counter("storage.flush.build.fallbacks"))
		require.Equal(t, []int64{200}, headTS(replayedHead(t, fs), benchSeries(0)), "checkpoint must leave the concurrent append replayable")
		require.Zero(t, pool.Waiting())
		release, ok := pool.TryAcquire(share)
		require.True(t, ok, "all flush workspace must return while the merge still holds its share")
		release()
		require.NoError(t, e.CloseWAL())
	})
}

func TestParallelPartBuildFailureRestoresFrozenStage(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"write", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				pool := memlimit.NewPool(128 << 20)
				file, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				be := &parallelPartGate{File: file, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{}),
					failOther: failure == "write"}
				resume := sync.OnceFunc(func() { close(be.resume) })
				defer resume()
				e, fs := newFaultWALEngine(t, 10)
				e.cfg.Backend, e.cfg.Prefix = be, "failed-parallel-parts"
				e.cfg.DiskSeries, e.cfg.MetricSeriesChunks = true, true
				e.cfg.ResidentHeadBytes, e.cfg.FlushMemoryBytes = 1<<30, 32<<20
				e.cfg.MaxPartBytes = 4 * partRowBytes
				e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
				e.head.disk = newDiskHeadState()
				for i := range 16 {
					_, err := e.Append(benchSeries(i), 100, float64(i))
					require.NoError(t, err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- e.Flush(ctx) }()
				<-be.entered
				if failure == "cancel" {
					<-be.other
					_, err := e.Append(benchSeries(0), 200, 2)
					require.NoError(t, err)
					cancel()
				}
				synctest.Wait()
				err = <-done
				if failure == "write" {
					require.ErrorIs(t, err, errParallelPartWrite)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.Zero(t, e.PartCount())
				require.Nil(t, e.flushing)
				require.Equal(t, snapshotHead(replayedHead(t, fs)), snapshotHead(e.head), "aborted work must remain entirely replayable")
				require.Zero(t, pool.Waiting())
				release, ok := pool.TryAcquire(pool.Total())
				require.True(t, ok, "joined builders must return every workspace grant")
				release()
				resume()
				be.failOther = false
				require.NoError(t, e.Flush(t.Context()), "retry must publish the restored stage")
				require.Zero(t, e.HeadSampleCount())
				require.NoError(t, e.CloseWAL())
			})
		})
	}
}

func TestParallelPartBuildUsesOneWorkerWithoutRoom(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pool := memlimit.NewPool(128 << 20)
		merge, err := pool.Acquire(t.Context(), 108<<20)
		require.NoError(t, err)
		defer merge()
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		be := &parallelPartGate{File: file, entered: make(chan struct{}), other: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e := New(Config{Backend: be, Prefix: "one-worker", DiskSeries: true, MetricSeriesChunks: true,
			ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 32 << 20, MaxPartBytes: 4 * partRowBytes,
			FlushAdmission: pool.Acquire, TryFlushAdmission: pool.TryAcquire})
		for i := range 16 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-be.entered
		synctest.Wait()
		select {
		case <-be.other:
			t.Error("second builder must not run without its aggregate memory reservation")
		default:
		}
		require.Zero(t, pool.Waiting(), "optional concurrency cannot queue behind its sibling")
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.Equal(t, 4, e.PartCount())
		require.Len(t, be.seen, 4)
		require.NoError(t, e.CloseWAL())
	})
}

func TestParallelPartBuildPreservesWeightedUnorderedRows(t *testing.T) {
	t.Parallel()
	file, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	pool := memlimit.NewPool(128 << 20)
	cfg := Config{Backend: file, Prefix: "weighted-parallel", DiskSeries: true, MetricSeriesChunks: true,
		AggregateStats: true, ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 32 << 20, MaxPartBytes: 7 * partRowBytes,
		FlushAdmission: pool.Acquire, TryFlushAdmission: pool.TryAcquire}
	e := New(cfg)
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	in := nodeExporterInputBatch(24, 0)
	for i := range in.series {
		s := in.series[i]
		s.Attributes = signal.NewAttributes(append(s.Attributes, signal.KeyValue{Key: []byte("typed"),
			Value: signal.MapValue(signal.KeyValue{Key: []byte("code"), Value: signal.IntValue(int64(i))})})...)
		id := s.Hash()
		ts, values, weights := []int64{100, 98, 100, 103, 101, 104}, []float64{0, 1, 2, 3, 4, 5}, []float64{1, 2, 1, 3, 1, 4}
		ids := make([]signal.SeriesID, len(ts))
		for j := range ids {
			ids[j] = id
		}
		res, err := e.AppendBatch(ids, ts, values, weights, func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, len(ids), res.Accepted)
	}
	read := func(e *Engine) map[signal.SeriesID]*fetch.Batch {
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 300})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		out := make(map[signal.SeriesID]*fetch.Batch)
		for i := range batches {
			out[batches[i].ID] = batches[i]
		}
		return out
	}
	want := read(e)
	check := func(actual map[signal.SeriesID]*fetch.Batch) {
		require.Len(t, actual, len(want))
		for id, expected := range want {
			got := actual[id]
			require.NotNil(t, got)
			require.True(t, expected.Series.Equal(got.Series), "typed identity %s", id)
			require.Equal(t, expected.Timestamps, got.Timestamps, "timestamps %s", id)
			require.Equal(t, expected.Values, got.Values, "duplicate precedence %s", id)
			require.Equal(t, expected.ScaleFactors, got.ScaleFactors, "sampling weights %s", id)
		}
	}
	require.NoError(t, e.Flush(t.Context()))
	check(read(e))
	require.NoError(t, e.CloseWAL())
	reopened := New(cfg)
	t.Cleanup(func() { require.NoError(t, reopened.CloseWAL()) })
	require.NoError(t, reopened.LoadPartsReadOnly(t.Context()))
	check(read(reopened))
}

func TestParallelPartBuildBoundsPlanSize(t *testing.T) {
	t.Parallel()
	file, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	pool := memlimit.NewPool(128 << 20)
	observability, measured := obstest.New(t)
	e := New(Config{Backend: file, Prefix: "bounded-plan", Obs: observability, DiskSeries: true, MetricSeriesChunks: true,
		ResidentHeadBytes: 1 << 30, FlushMemoryBytes: 32 << 20, MaxPartBytes: partRowBytes,
		FlushAdmission: pool.Acquire, TryFlushAdmission: pool.TryAcquire})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	for i := range maxHeadPartJobs + 1 {
		_, err := e.Append(benchSeries(i), 100, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Equal(t, maxHeadPartJobs+1, e.PartCount())
	require.EqualValues(t, 1, measured.Counter("storage.flush.build.fallbacks", "reason", "plan"))
	release, ok := pool.TryAcquire(pool.Total())
	require.True(t, ok)
	release()
}

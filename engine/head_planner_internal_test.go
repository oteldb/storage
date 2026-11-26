package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
)

func TestFixedHeadRotationUsesSeparateCredit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		be := &generationGateBackend{
			Backend: backend.Memory(), first: make(chan struct{}), resume: make(chan struct{}),
		}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e.cfg.Backend, e.cfg.Prefix = be, "rotation"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		for i := range 16 {
			_, err := e.Append(benchSeries(i), 100, 1)
			require.NoError(t, err)
		}
		e.cfg.HeadRotationBytes = e.head.ownedIdentityBytes() + e.head.bytes
		require.NoError(t, e.flushResidentHead())
		<-be.first
		synctest.Wait()
		require.Len(t, e.pressureGenerations, 1)
		require.Zero(t, e.head.bytes)
		for i := range 16 {
			_, err := e.Append(benchSeries(i), 200, 2)
			require.NoError(t, err)
		}
		require.NoError(t, e.flushResidentHead())
		synctest.Wait()
		require.Len(t, e.pressureGenerations, 2)
		require.True(t, e.pressureGenerations[1].ready)
		require.Zero(t, e.flushedEpoch)
		resume()
		synctest.Wait()
		require.NoError(t, e.Close(t.Context()))
	})
}

type plannerBuildGate struct {
	*backendfile.File

	descriptor, numeric, resume chan struct{}
	descriptorOnce, numericOnce sync.Once
}

func (b *plannerBuildGate) CreateObjectDeferred(ctx context.Context, key string) (backend.ObjectWriter, error) {
	if strings.HasSuffix(key, "/series-index") {
		b.descriptorOnce.Do(func() { close(b.descriptor) })
		select {
		case <-b.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.File.CreateObjectDeferred(ctx, key)
}

func (b *plannerBuildGate) WriteDeferred(ctx context.Context, key string, data []byte) error {
	if err := b.File.WriteDeferred(ctx, key, data); err != nil {
		return err
	}
	if strings.HasSuffix(key, "/series-ref") {
		b.numericOnce.Do(func() { close(b.numeric) })
	}
	return nil
}

func TestHeadPlannerBuildsNumericWhileDescriptorsAreBlocked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		gate := &plannerBuildGate{File: file, descriptor: make(chan struct{}), numeric: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(gate.resume) })
		defer resume()
		e, fs := newFaultWALEngine(t, 10)
		e.cfg.Backend, e.cfg.Prefix = gate, "planner"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.SeparateSeries, e.cfg.MetricSeriesChunks = true, true
		e.cfg.ResidentHeadBytes, e.cfg.HeadRotationBytes, e.cfg.FlushMemoryBytes = 1<<30, 256<<20, 64<<20
		pool := memlimit.NewPool(512 << 20)
		e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
		e.head.disk = newDiskHeadState()
		for i := range 16 {
			_, err := e.Append(benchSeries(i), 100, 1)
			require.NoError(t, err)
		}
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()); close(done) }()
		defer func() {
			resume()
			<-done
		}()
		select {
		case <-gate.descriptor:
		case err := <-done:
			t.Fatalf("flush finished before reaching descriptor write: %v", err)
		}
		synctest.Wait()
		select {
		case <-gate.numeric:
		default:
			t.Fatal("numeric output did not reach its writer before descriptor completion")
		}
		require.Zero(t, e.PartCount())
		require.Zero(t, e.flushedEpoch)
		require.Len(t, replayedHead(t, fs).samples, 16)
		resume()
		require.NoError(t, <-done)
		require.NoError(t, e.LoadParts(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, batches, 16)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestSelectiveCountFitsLargeHeadBudget(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), DiskSeries: true, ResidentHeadBytes: 1 << 30})
	defer func() { require.NoError(t, e.Close(t.Context())) }()
	in := nodeExporterInputBatch(4096, 0)
	res := in.append(e)
	require.NoError(t, res.err)
	require.Equal(t, 4096, res.result.Accepted)
	instance := fetch.EqualMatcher{Name: "instance", Value: cpuHost(0)}
	r := fetch.Request{
		Start: cpuFixtureBase, End: cpuFixtureBase,
		Matchers: []fetch.Matcher{{Name: []byte(instance.Name), Match: instance.Predicate(), Spec: &instance}},
	}
	n, err := e.Count(readbudget.With(t.Context(), readbudget.New(64<<10)), r)
	require.NoError(t, err)
	require.Len(t, cpuFixtureModes, n)
}

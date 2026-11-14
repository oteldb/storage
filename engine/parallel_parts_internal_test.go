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
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
)

type parallelPartGate struct {
	*backendfile.File

	mu                     sync.Mutex
	first                  string
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
	if strings.Contains(key, "/c/") {
		prefix := strings.Split(key, "/c/")[0]
		b.mu.Lock()
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
		}
	}
	return nil
}

func TestParallelPartBuildsReachWritersBeforePublication(t *testing.T) {
	reproduce.Unfixed(t, 661, "one frozen head builds output parts sequentially")
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
		require.Equal(t, []int64{200}, headTS(replayedHead(t, fs), benchSeries(0)), "checkpoint must leave the concurrent append replayable")
		require.Zero(t, pool.Waiting())
		release, ok := pool.TryAcquire(share)
		require.True(t, ok, "all flush workspace must return while the merge still holds its share")
		release()
		require.NoError(t, e.CloseWAL())
	})
}

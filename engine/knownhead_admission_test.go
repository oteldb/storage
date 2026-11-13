package engine

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
)

func TestMixedKnownFlushUsesOneAdmissionLease(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		e := knownHeadEngine(t, files)
		e.cfg.FlushMemoryBytes = 448 << 20
		knownHeadAppend(t, e, benchSeries(1), 10, 1)
		require.NoError(t, e.Flush(t.Context()))
		knownHeadAppend(t, e, benchSeries(1), 100, 2)
		large := benchSeries(2)
		large.Resource.SchemaURL = []byte(strings.Repeat("x", 1<<20))
		knownHeadAppend(t, e, large, 100, 3)
		pool := memlimit.NewPool(64 << 20)
		e.cfg.FlushAdmission = pool.Acquire
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- e.Flush(ctx) }()
		synctest.Wait()
		select {
		case err := <-done:
			require.NoError(t, err)
		default:
			cancel()
			<-done
			t.Fatal("retained partition state must not deadlock a full-pool metadata reservation")
		}
		require.Equal(t, 0, e.HeadSampleCount())
		require.Nil(t, e.head.known)
		require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 2)
	})
}

func TestKnownNativeFlushReservesItsShape(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := knownHeadEngine(t, files)
	e.cfg.FlushMemoryBytes = 448 << 20
	for i := range 8192 {
		knownHeadAppend(t, e, benchSeries(i), 10, 1)
	}
	require.NoError(t, e.Flush(t.Context()))
	for i := range 8192 {
		knownHeadAppend(t, e, benchSeries(i), 100, 2)
	}
	before := e.PartCount()
	var peak int64
	e.cfg.FlushAdmission = func(_ context.Context, n int64) (func(), error) { peak = max(peak, n); return func() {}, nil }
	require.NoError(t, e.Flush(t.Context()))
	require.Equal(t, 1, e.PartCount()-before)
	require.Positive(t, peak)
	require.Less(t, peak, int64(96<<20), "known samples must not reserve the process-sized part target")
	require.Zero(t, e.head.series.Len())
}

func TestKnownFlushUsesReliefShareWhileMergeRuns(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := knownHeadEngine(t, backend.Memory())
		knownHeadAppend(t, e, benchSeries(1), 10, 1)
		require.NoError(t, e.Flush(t.Context()))
		knownHeadAppend(t, e, benchSeries(1), 100, 2)
		pool := memlimit.NewPool(64 << 20)
		release, err := pool.Acquire(t.Context(), 48<<20)
		require.NoError(t, err)
		defer release()
		e.cfg.FlushMemoryBytes = 16 << 20
		e.cfg.FlushAdmission = pool.Acquire
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- e.Flush(ctx) }()
		synctest.Wait()
		select {
		case err := <-done:
			require.NoError(t, err)
		default:
			cancel()
			<-done
			t.Fatal("known flush lost its reserved relief share")
		}
	})
}

package engine

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
)

func TestPressureMergePrefixPrecedence(t *testing.T) {
	t.Parallel()
	for _, generations := range []int{1, 2} {
		t.Run(string(rune('0'+generations)), func(t *testing.T) {
			t.Parallel()
			if generations == 1 {
				reproduce.Unfixed(t, 661, "a merge started after an overlapping frozen flush can override the newer write after catalog reload")
			}
			synctest.Test(t, func(t *testing.T) {
				e, _ := newFaultWALEngine(t, 10)
				memory := backend.Memory()
				observability, metrics := obstest.New(t)
				e.cfg.Obs = observability
				e.cfg.Backend, e.cfg.Prefix = memory, "merge-prefix"
				e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, generations
				e.cfg.ResidentHeadBytes = 1 << 30
				e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
				e.head.disk = newDiskHeadState()
				s := benchSeries(0)
				for range 2 {
					_, err := e.Append(s, 100, 1)
					require.NoError(t, err)
					require.NoError(t, e.Flush(t.Context()))
				}
				be := &generationGateBackend{Backend: memory, first: make(chan struct{}), resume: make(chan struct{})}
				resume := sync.OnceFunc(func() { close(be.resume) })
				defer resume()
				e.cfg.Backend = be
				_, err := e.Append(s, 100, 2)
				require.NoError(t, err)
				done := make(chan error, 1)
				go func() { done <- e.Flush(t.Context()) }()
				<-be.first
				err = e.MergeWith(t.Context(), MergeOptions{Force: true})
				if generations == 2 {
					require.ErrorIs(t, err, errPressureMergeOrder)
					require.EqualValues(t, 1, metrics.Counter("storage.merge.order_deferred"))
					require.Zero(t, metrics.Counter("storage.merge.deferred"), "ordering waits are not memory refusals")
				} else {
					require.NoError(t, err)
				}
				resume()
				synctest.Wait()
				require.NoError(t, <-done)
				for _, reload := range []bool{false, true} {
					if reload {
						require.NoError(t, e.LoadParts(t.Context()))
					}
					it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
					require.NoError(t, err)
					batches, err := fetch.Drain(t.Context(), it)
					require.NoError(t, err)
					require.Len(t, batches, 1)
					require.Equal(t, []float64{2}, batches[0].Values, "later accepted value must survive reload")
				}
				require.NoError(t, e.Close(t.Context()))
			})
		})
	}
}

func TestPressureMergeAllowsDisjointTimeOverlap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, _ := newFaultWALEngine(t, 10)
		memory := backend.Memory()
		e.cfg.Backend, e.cfg.Prefix = memory, "disjoint-merge"
		e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
		e.cfg.ResidentHeadBytes = 1 << 30
		e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
		e.head.disk = newDiskHeadState()
		s := benchSeries(0)
		for _, ts := range []int64{100, 200} {
			_, err := e.Append(s, ts, float64(ts))
			require.NoError(t, err)
			require.NoError(t, e.Flush(t.Context()))
		}
		be := &generationGateBackend{Backend: memory, first: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e.cfg.Backend = be
		_, err := e.Append(s, 300, 300)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- e.Flush(t.Context()) }()
		<-be.first
		changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
		require.NoError(t, err)
		require.True(t, changed, "disjoint immutable ranges must merge while generation is blocked")
		resume()
		synctest.Wait()
		require.NoError(t, <-done)
		require.NoError(t, e.LoadParts(t.Context()))
		it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 400})
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Equal(t, []float64{100, 200, 300}, batches[0].Values)
		require.NoError(t, e.Close(t.Context()))
	})
}

func TestPressureMergeSameTimeDisjointSeriesProgress(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 10)
	e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "split-progress"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.ResidentHeadBytes = 1 << 30
	e.cfg.MaxPartBytes = partRowBytes
	e.head.disk = newDiskHeadState()
	for i := range 64 {
		_, err := e.Append(benchSeries(i), 100, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Greater(t, e.PartCount(), 2)
	e.cfg.MaxPartBytes = 0
	for attempt := 0; e.PartCount() > 1 && attempt < 64; attempt++ {
		before := e.PartCount()
		changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true})
		require.NoError(t, err)
		require.True(t, changed)
		require.Less(t, e.PartCount(), before)
	}
	require.Equal(t, 1, e.PartCount(), "equal timestamps with disjoint series bounds must converge")
	require.NoError(t, e.Close(t.Context()))
}

func TestPressureMergeRevalidatesAfterNewerFlush(t *testing.T) {
	t.Parallel()
	for _, prefixBeforeFreeze := range []bool{false, true} {
		name := "late prefix rejected"
		if prefixBeforeFreeze {
			name = "early prefix keeps overlap"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				e, _ := newFaultWALEngine(t, 10)
				memory := backend.Memory()
				e.cfg.Backend, e.cfg.Prefix = memory, "late-generation"
				e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
				e.cfg.ResidentHeadBytes = 1 << 30
				e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
				e.head.disk = newDiskHeadState()
				s := benchSeries(0)
				for range 2 {
					_, err := e.Append(s, 100, 1)
					require.NoError(t, err)
					require.NoError(t, e.Flush(t.Context()))
				}
				entered, resume := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				defer release()
				if prefixBeforeFreeze {
					e.cfg.Backend = &generationGateBackend{Backend: memory, first: entered, resume: resume}
				} else {
					e.cfg.MergeAdmission = func(context.Context, int64, bool) (func(), bool, error) {
						close(entered)
						<-resume
						return func() {}, true, nil
					}
				}
				done := make(chan error, 1)
				go func() { done <- e.MergeWith(t.Context(), MergeOptions{Force: true}) }()
				<-entered
				_, err := e.Append(s, 100, 2)
				require.NoError(t, err)
				require.NoError(t, e.Flush(t.Context()))
				release()
				synctest.Wait()
				err = <-done
				if prefixBeforeFreeze {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errPressureMergeOrder)
				}
				require.Empty(t, e.retiring, "abandoned outputs release readers and retire without remaining ownership")
				require.NoError(t, e.LoadParts(t.Context()))
				it, err := e.Fetch(t.Context(), fetch.Request{Start: 100, End: 100})
				require.NoError(t, err)
				batches, err := fetch.Drain(t.Context(), it)
				require.NoError(t, err)
				require.Len(t, batches, 1)
				require.Equal(t, []float64{2}, batches[0].Values)
				e.cfg.MergeAdmission = nil
				require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
				require.Equal(t, 1, e.PartCount(), "a late rejection must not permanently prevent compaction")
				require.NoError(t, e.Close(t.Context()))
			})
		})
	}
}

package storage

import (
	"context"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMaintenanceEventPriority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                       string
		stop, flush, tick, pending bool
		want                       maintenanceEvent
	}{
		{"shutdown", true, true, true, true, maintenanceStop},
		{"pressure flush", false, true, false, true, maintenanceFlush},
		{"timed cycle", false, false, true, true, maintenanceTick},
		{"catchup", false, false, false, true, maintenanceMerge},
		{"idle shutdown", true, false, false, false, maintenanceStop},
		{"idle flush", false, true, false, false, maintenanceFlush},
		{"idle cycle", false, false, true, false, maintenanceTick},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stop, flush := make(chan struct{}), make(chan struct{}, 1)
			tick := make(chan time.Time, 1)
			if tc.stop {
				close(stop)
			}
			if tc.flush {
				flush <- struct{}{}
			}
			if tc.tick {
				tick <- time.Time{}
			}
			assert.Equal(t, tc.want, nextMaintenanceEvent(stop, flush, tick, tc.pending, false))
		})
	}
}

func TestMaintenanceEventWaitsWithoutWork(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		stop := make(chan struct{})
		done := make(chan maintenanceEvent, 1)
		go func() { done <- nextMaintenanceEvent(stop, nil, nil, false, false) }()
		synctest.Wait()
		select {
		case event := <-done:
			t.Fatalf("idle loop returned event %v without a wake", event)
		default:
		}
		close(stop)
		assert.Equal(t, maintenanceStop, <-done)
	})
}

func TestMergeCatchupBoundedRotation(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{1, 3, 16} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			t.Parallel()
			calls := make([]int, 8)
			tasks := make([]mergeTask, len(calls))
			for i := range tasks {
				tasks[i].run = func() mergeTurn {
					calls[i]++
					return mergeTurn{more: true, progress: true}
				}
			}
			for range (len(calls) + limit - 1) / limit {
				total := 0
				for _, n := range calls {
					total += n
				}
				tasks = runMergeTasks(tasks, limit)
				after := 0
				for _, n := range calls {
					after += n
				}
				assert.LessOrEqual(t, after-total, limit, "one turn must remain bounded")
			}
			for i, n := range calls {
				assert.Positive(t, n, "engine %d never received a turn", i)
			}
		})
	}
}

func TestMergeCatchupBacksOffWithoutProgress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		result mergeTurn
	}{
		{"admission busy", mergeTurn{more: true, deferred: true}},
		{"no progress", mergeTurn{more: true}},
		{"error or no candidates", mergeTurn{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			tasks := []mergeTask{{run: func() mergeTurn { calls++; return tc.result }}}
			assert.Empty(t, runMergeTasks(tasks, 16))
			assert.Equal(t, 1, calls)
		})
	}
}

func TestMergeCatchupRetriesDeferredAlone(t *testing.T) {
	t.Parallel()
	pool := memlimit.NewPool(100)
	var retried bool
	tasks := []mergeTask{
		{run: func() mergeTurn { return mergeTurn{progress: true} }},
		{run: func() mergeTurn {
			if !retried {
				retried = true
				return mergeTurn{more: true, deferred: true}
			}
			release, ok := pool.TryAcquire(100)
			assert.True(t, ok, "the deferred retry can take the whole budget")
			if ok {
				release()
			}
			return mergeTurn{progress: true}
		}},
	}
	tasks = runMergeTasks(tasks, 16)
	require.Len(t, tasks, 1)
	assert.True(t, tasks[0].deferred)
	fresh := false
	tasks = append(tasks, mergeTask{run: func() mergeTurn { fresh = true; return mergeTurn{progress: true} }})
	tasks = runMergeTasks(tasks, 16)
	assert.False(t, fresh, "a deferred turn runs without other maintenance contenders")
	require.Len(t, tasks, 1)
	assert.Empty(t, runMergeTasks(tasks, 16))
	assert.True(t, fresh)
}

func TestMaintenanceCatchesUpBetweenTicks(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, err := InMemory(WithMergeMemory(-1))
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close(ctx)) }()

		for day := range 3 {
			for row := range 2 {
				ts := int64(day)*int64(24*time.Hour) + int64(row+1)
				_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{ts}, []float64{float64(ts)}))
				require.NoError(t, err)
				_, err = s.WriteLogs(ctx, logBatch("api", [3]any{int(ts), 9, "record"}))
				require.NoError(t, err)
				require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
				require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))
			}
		}

		s.stopCh = make(chan struct{})
		s.flushWake = make(chan struct{}, 1)
		s.wg.Add(1)
		go s.runMaintenance(ctx, time.Hour)
		time.Sleep(time.Hour)
		synctest.Wait()

		st := mustTenant(t, s.Inspect(), "default")
		for _, sig := range []signal.Signal{signal.Metric, signal.Log} {
			stats, ok := findSignal(st, sig)
			require.True(t, ok)
			assert.Equal(t, 3, stats.Parts, "%s must drain all three buckets before the next tick", sig)
		}
		assert.Equal(t, int64(1), s.Inspect().Maintenance.Cycles)
		assert.Len(t, logBodies(t, s.LogFetcher("default"), fetch.Request{Start: 0, End: int64(72 * time.Hour)}), 6)
	})
}

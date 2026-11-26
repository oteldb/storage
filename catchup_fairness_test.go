package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestCatchupRunsBetweenOverdueTicks(t *testing.T) {
	t.Parallel()
	stop := make(chan struct{})
	tick := make(chan time.Time, 1)
	calls := make([]int, 4)
	fresh := func() []mergeTask {
		tasks := make([]mergeTask, len(calls))
		for i := range calls {
			tasks[i] = mergeTask{key: mergeTaskKey{tenant: signal.TenantID(string(rune('a' + i))), signal: signal.Log}, run: func() mergeTurn {
				calls[i]++
				return mergeTurn{more: true, progress: true}
			}}
		}
		return tasks
	}
	var pending []mergeTask
	for range calls {
		tick <- time.Time{}
		require.Equal(t, maintenanceTick, nextMaintenanceEvent(stop, nil, tick, len(pending) > 0, false))
		pending = refreshMergeTasks(pending, fresh())
		tick <- time.Time{}
		require.Equal(t, maintenanceMerge, nextMaintenanceEvent(stop, nil, tick, true, true))
		pending = runMergeTasks(pending, 1)
		<-tick
	}
	assert.Equal(t, []int{1, 1, 1, 1}, calls)
	close(stop)
	assert.Equal(t, maintenanceStop, nextMaintenanceEvent(stop, nil, tick, true, true))
}

func TestRefreshMergeTasks(t *testing.T) {
	t.Parallel()
	a := mergeTaskKey{tenant: "a", signal: signal.Log}
	b := mergeTaskKey{tenant: "b", signal: signal.Exemplar}
	c := mergeTaskKey{tenant: "c", signal: signal.Metric}
	for _, tt := range []struct {
		name           string
		pending, fresh []mergeTask
		want           []mergeTaskKey
	}{
		{"initial", nil, []mergeTask{{key: a}, {key: b}}, []mergeTaskKey{a, b}},
		{"preserve rotation", []mergeTask{{key: b}, {key: a}}, []mergeTask{{key: a}, {key: b}, {key: c}}, []mergeTaskKey{b, a, c}},
		{"drop completed", []mergeTask{{key: b}, {key: a}}, []mergeTask{{key: a}, {key: c}}, []mergeTaskKey{a, c}},
		{"empty", []mergeTask{{key: a}}, nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got []mergeTaskKey
			for _, task := range refreshMergeTasks(tt.pending, tt.fresh) {
				got = append(got, task.key)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDescriptorAndSampleWorkHaveSeparateTurns(t *testing.T) {
	t.Parallel()
	var done []string
	makeTask := func(descriptor bool, name string) mergeTask {
		return mergeTask{key: mergeTaskKey{tenant: "same", signal: signal.Metric, descriptor: descriptor},
			run: func() mergeTurn { done = append(done, name); return mergeTurn{progress: true} }}
	}
	samples, descriptors := makeTask(false, "samples"), makeTask(true, "descriptors")
	tasks := refreshMergeTasks(nil, []mergeTask{samples, descriptors})
	require.Len(t, tasks, 2)
	tasks = runMergeTasks(tasks, 8)
	require.Equal(t, []string{"samples"}, done)
	require.Empty(t, runMergeTasks(tasks, 8))
	require.Equal(t, []string{"samples", "descriptors"}, done)
}

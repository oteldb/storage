package storage

import (
	"context"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

type maintenanceEvent uint8

const (
	maintenanceStop maintenanceEvent = iota
	maintenanceTick
	maintenanceFlush
	maintenanceMerge
)

func nextMaintenanceEvent(stop, flush <-chan struct{}, tick <-chan time.Time, pending, preferMerge bool) maintenanceEvent {
	select {
	case <-stop:
		return maintenanceStop
	default:
	}
	if pending && preferMerge {
		return maintenanceMerge
	}
	select {
	case <-stop:
		return maintenanceStop
	case <-tick:
		return maintenanceTick
	case <-flush:
		return maintenanceFlush
	default:
		if pending {
			return maintenanceMerge
		}
	}
	select {
	case <-stop:
		return maintenanceStop
	case <-tick:
		return maintenanceTick
	case <-flush:
		return maintenanceFlush
	}
}

type mergeTask struct {
	run      func() mergeTurn
	deferred bool
	key      mergeTaskKey
}

type mergeTaskKey struct {
	tenant     signal.TenantID
	signal     signal.Signal
	descriptor bool
}

type mergeTurn struct {
	more, deferred, progress bool
}

func refreshMergeTasks(pending, fresh []mergeTask) []mergeTask {
	byKey := make(map[mergeTaskKey]mergeTask, len(fresh))
	for _, task := range fresh {
		byKey[task.key] = task
	}
	queue := make([]mergeTask, 0, len(fresh))
	for _, task := range pending {
		if current, ok := byKey[task.key]; ok {
			queue = append(queue, current)
			delete(byKey, task.key)
		}
	}
	for _, task := range fresh {
		if _, ok := byKey[task.key]; ok {
			queue = append(queue, task)
			delete(byKey, task.key)
		}
	}
	return queue
}

func runMergeTasks(tasks []mergeTask, limit int) []mergeTask {
	n := min(len(tasks), max(limit, 1))
	for i := range n {
		if tasks[i].key.descriptor != tasks[0].key.descriptor {
			n = i
			break
		}
		if tasks[i].deferred {
			n = max(i, 1)
			break
		}
	}
	results := make([]mergeTurn, n)
	parallel.ForEach(n, n, func(i int) { results[i] = tasks[i].run() })

	progress := false
	for _, r := range results {
		progress = progress || r.progress
	}
	remaining := tasks[n:]
	for i, r := range results {
		if r.more && (r.progress || (r.deferred && progress)) {
			task := tasks[i]
			task.deferred = r.deferred
			remaining = append(remaining, task)
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return remaining
}

func (s *Storage) mergeCatchupTasks(ctx context.Context) []mergeTask {
	var tasks []mergeTask
	for tid, eng := range s.engineSnapshotByTenant() {
		if !s.claimsShard(tid) || s.metricShape(tid, eng, s.sizeCutoffFor(ctx, tenantOfShard(tid))).Candidates == 0 {
			continue
		}
		tasks = append(tasks, mergeTask{
			key:      mergeTaskKey{tenant: tid, signal: signal.Metric},
			deferred: eng.MergeDeferred(),
			run:      func() mergeTurn { return s.catchupMetrics(ctx, tid, eng) },
		})
	}
	add := func(engines map[signal.TenantID]*recordengine.Engine, sig signal.Signal, prefix string) {
		for tid, eng := range engines {
			if !s.claimsShard(tid) || s.recordShape(sig, tid, eng, s.sizeCutoffFor(ctx, tenantOfShard(tid))).Candidates == 0 {
				continue
			}
			tasks = append(tasks, mergeTask{key: mergeTaskKey{tenant: tid, signal: sig}, deferred: eng.MergeDeferred(), run: func() mergeTurn {
				return s.catchupRecords(ctx, tid, eng, sig, prefix)
			}})
		}
	}
	add(s.logEngineSnapshotByTenant(), signal.Log, logsPrefix)
	add(s.traceEngineSnapshotByTenant(), signal.Trace, tracesPrefix)
	add(s.profileEngineSnapshotByTenant(), signal.Profile, profilesPrefix)
	add(s.exemplarEngineSnapshotByTenant(), signal.Exemplar, exemplarsPrefix)
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].deferred && !tasks[j].deferred })
	return tasks
}

func (s *Storage) descriptorMergeTasks(ctx context.Context) []mergeTask {
	var tasks []mergeTask
	for tid, eng := range s.engineSnapshotByTenant() {
		if !eng.NeedsSeriesCompaction() || !s.claimsShard(tid) {
			continue
		}
		tasks = append(tasks, mergeTask{
			key: mergeTaskKey{tenant: tid, signal: signal.Metric, descriptor: true},
			run: func() mergeTurn {
				if !s.claimsShard(tid) {
					return mergeTurn{}
				}
				changed, err := eng.MergeSeriesStep(ctx, true)
				if err != nil {
					s.obs.Logger(ctx).Warn("merge metric descriptors", zap.String("tenant", string(tid)), zap.Error(err))
				}
				return mergeTurn{progress: changed}
			},
		})
	}
	return tasks
}

func (s *Storage) catchupMetrics(ctx context.Context, tid signal.TenantID, eng *engine.Engine) mergeTurn {
	if !s.claimsShard(tid) {
		return mergeTurn{}
	}
	opts := s.metricMergeOptions(tid, s.sizeCutoffFor(ctx, tenantOfShard(tid)).at(signal.Metric))
	opts.Background = true
	if eng.MergeShapeWith(opts).Candidates == 0 {
		return mergeTurn{}
	}
	changed, err := eng.MergeSamplesStep(ctx, opts)
	if err != nil {
		return mergeTurn{}
	}
	if eng.MergeDeferred() {
		return mergeTurn{more: true, deferred: true, progress: changed}
	}
	if !changed {
		return mergeTurn{}
	}
	if _, err := eng.PruneIdentities(ctx); err != nil {
		return mergeTurn{}
	}
	s.notifyPeers(ctx, tid, metricsPrefix)
	return mergeTurn{more: eng.MergeShapeWith(opts).Candidates > 0, progress: true}
}

func (s *Storage) catchupRecords(
	ctx context.Context, tid signal.TenantID, eng *recordengine.Engine, sig signal.Signal, prefix string,
) mergeTurn {
	if !s.claimsShard(tid) {
		return mergeTurn{}
	}
	opts := recordengine.MergeOptions{
		RetainFrom: s.retainFrom(tid, sig, s.sizeCutoffFor(ctx, tenantOfShard(tid)).at(sig)),
		Background: true,
	}
	before := eng.MergeShapeWith(opts)
	if before.Candidates == 0 {
		return mergeTurn{}
	}
	changed, err := eng.MergeStep(ctx, opts)
	if err != nil {
		return mergeTurn{}
	}
	if eng.MergeDeferred() {
		return mergeTurn{more: true, deferred: true, progress: changed}
	}
	if !changed {
		after := eng.MergeShapeWith(opts)
		learned := after.NonReducingMerges != before.NonReducingMerges
		return mergeTurn{more: learned && after.Candidates > 0, progress: learned}
	}
	if _, err := eng.PruneIdentities(ctx); err != nil {
		return mergeTurn{}
	}
	s.notifyPeers(ctx, tid, prefix)
	return mergeTurn{more: eng.MergeShapeWith(opts).Candidates > 0, progress: true}
}

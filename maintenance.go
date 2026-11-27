package storage

import (
	"context"
	"sort"

	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

type maintTask struct {
	run      func()
	pressure int64
	deferred bool
}

func sortMaintTasks(tasks []maintTask) {
	sort.SliceStable(tasks, func(i, j int) bool {
		if tasks[i].deferred != tasks[j].deferred {
			return tasks[i].deferred
		}

		return tasks[i].pressure > tasks[j].pressure
	})
}

func runMaintTasks(tasks []maintTask, limit int) {
	sortMaintTasks(tasks)
	for len(tasks) > 0 && tasks[0].deferred {
		tasks[0].run()
		tasks = tasks[1:]
	}

	parallel.ForEach(len(tasks), limit, func(i int) { tasks[i].run() })
}

func (s *Storage) appendRecordMaintTasks(
	ctx context.Context, tasks []maintTask, engines map[signal.TenantID]*recordengine.Engine,
	sig signal.Signal, signalPrefix string, owned map[signal.TenantID]struct{},
	sizeCutoffs map[signal.TenantID]sizeRetentionDecision,
) []maintTask {
	coldRecord := func(eng *recordengine.Engine) []ecPartRef {
		ps := eng.Parts()
		refs := make([]ecPartRef, len(ps))
		for i, p := range ps {
			refs[i] = ecPartRef{prefix: p.ID, maxTime: p.MaxTime}
		}

		return refs
	}
	// mergeRecords / refreshRecords mirror the metric pair: the prune follows whatever changed the
	// part set, since that is the only thing that kills a stream identity. A replica reaches it
	// only through the refresh — it never merges.
	mergeRecords := func(tid signal.TenantID, eng *recordengine.Engine, sig signal.Signal) error {
		decision := s.retentionFor(tid, sig, sizeCutoffs[tid].at(sig))
		opts := recordengine.MergeOptions{
			RetainFrom: decision.cutoff, RetentionBoundBy: decision.bound,
			Background: true, // Skip if the merge budget is busy.
		}

		if err := eng.MergeWith(ctx, opts); err != nil {
			return err
		}

		_, err := eng.PruneIdentities(ctx)

		return err
	}

	refreshRecords := func(eng *recordengine.Engine) error {
		if err := eng.RefreshReplica(ctx); err != nil {
			return err
		}

		_, err := eng.PruneIdentities(ctx)

		return err
	}

	for tid, eng := range engines {
		tasks = append(tasks, maintTask{pressure: eng.HeadBytes(), deferred: eng.MergeDeferred(), run: func() {
			s.maintainOneEngine(ctx, tid, signalPrefix, owned, engineOps{
				flush:   func() error { return eng.Flush(ctx) },
				merge:   func() error { return mergeRecords(tid, eng, sig) },
				refresh: func() error { return refreshRecords(eng) },
				reload:  func() error { return eng.ReloadFenced(ctx) },
				adopt:   eng.AdoptWants,
				ecParts: func() []ecPartRef { return coldRecord(eng) },
			})
		}})
	}
	return tasks
}

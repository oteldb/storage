package engine

import (
	"context"

	"github.com/oteldb/storage/internal/buildbudget"
)

// Preflight uses separate, temporary grants. Never park behind a memory
// waiter while holding the plan's memory.
func (p *headPartPlan) prepareSharedIdentities(ctx context.Context, planner *headPartBuilder) error {
	var largest int
	for _, j := range p.jobs {
		largest = max(largest, len(j.entries))
	}
	if largest == 0 {
		return nil
	}
	e := planner.e
	release, ok := e.cfg.TryFlushAdmission(buildbudget.Mul(int64(largest), 4))
	if !ok {
		return errHeadPlanUnavailable
	}
	defer release()
	refs := make([]uint32, largest)
	for _, j := range p.jobs {
		for i, entry := range j.entries {
			refs[i] = entry.ref
		}
		ids := identitySnapshot{view: planner.view, refs: refs[:len(j.entries)]}
		if err := prepareSharedHeadJob(ctx, e, ids); err != nil {
			return err
		}
	}
	return nil
}

func prepareSharedHeadJob(ctx context.Context, e *Engine, ids identitySnapshot) error {
	var held int64
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	return e.ensureSeriesIdentities(ctx, ids, func(bytes int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if bytes <= held {
			return nil
		}
		want := max(bytes, buildbudget.Add(held, held/2))
		release, ok := e.cfg.TryFlushAdmission(want - held)
		if !ok && want != bytes {
			want = bytes
			release, ok = e.cfg.TryFlushAdmission(want - held)
		}
		if !ok {
			return errHeadPlanUnavailable
		}
		releases = append(releases, release)
		held = want
		return nil
	})
}

package engine

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/buildbudget"
)

func (p *headPartPlan) buildHeadParts(ctx context.Context, planner *headPartBuilder) ([]*part, int, error) {
	e := planner.e
	if e.cfg.SeparateSeries {
		if e.fixedHeadRotation() {
			parts, rows, err := p.buildSharedConcurrently(ctx, planner)
			if !errors.Is(err, errHeadPlanUnavailable) {
				return parts, rows, err
			}
		}
		if err := p.prepareSharedIdentities(ctx, planner, 0); err != nil {
			return nil, 0, err
		}
		planner.sharedIdentitiesReady = true
	}
	return p.build(ctx, planner)
}

func (p *headPartPlan) buildSharedConcurrently(ctx context.Context, planner *headPartBuilder) ([]*part, int, error) {
	admission, err := p.admit(planner)
	if err != nil {
		return nil, 0, err
	}
	if admission.release != nil {
		defer admission.release()
	}
	// Both grants are held before dispatch. Descriptor growth can fall back to serial preflight.
	metadata := planner.target
	release, ok := planner.e.cfg.TryFlushAdmission(metadata)
	if !ok {
		return nil, 0, errHeadPlanUnavailable
	}
	defer release()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	planner.sharedIdentitiesReady, planner.deferSharedBinding = true, true
	defer func() { planner.sharedIdentitiesReady, planner.deferSharedBinding = false, false }()
	done := make(chan error, 1)
	go func() {
		err := p.prepareSharedIdentities(ctx, planner, metadata)
		if err != nil {
			cancel()
		}
		done <- err
	}()
	parts, rows, numericErr := p.buildAdmitted(ctx, planner, admission)
	if numericErr != nil {
		cancel()
	}
	metadataErr := <-done
	err = numericErr
	if metadataErr != nil && (err == nil || errors.Is(err, context.Canceled)) {
		err = metadataErr
	}
	if err == nil {
		err = planner.e.bindPrivateHeadParts(ctx, parts)
	}
	if err != nil {
		closePartReaders(parts)
		for _, output := range parts {
			planner.e.discardPrivateHeadPart(ctx, output.prefix)
		}
		return nil, 0, err
	}
	return parts, rows, nil
}

func (e *Engine) bindPrivateHeadParts(ctx context.Context, parts []*part) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, part := range parts {
		if err := attachSharedSeriesFrom(part, e.seriesTable); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) discardPrivateHeadPart(ctx context.Context, prefix string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_ = deletePart(ctx, e.cfg.Backend, prefix)
}

// Preflight uses separate, temporary grants. Never park behind a memory
// waiter while holding the plan's memory.
func (p *headPartPlan) prepareSharedIdentities(ctx context.Context, planner *headPartBuilder, budget int64) error {
	var largest int
	for _, j := range p.jobs {
		largest = max(largest, len(j.entries))
	}
	if largest == 0 {
		return nil
	}
	e := planner.e
	references := buildbudget.Mul(int64(largest), 4)
	if budget == 0 {
		release, ok := e.cfg.TryFlushAdmission(references)
		if !ok {
			return errHeadPlanUnavailable
		}
		defer release()
	} else if references >= budget {
		return errHeadPlanUnavailable
	}

	refs := make([]uint32, largest)
	for _, j := range p.jobs {
		for i, entry := range j.entries {
			refs[i] = entry.ref
		}
		ids := identitySnapshot{view: planner.view, refs: refs[:len(j.entries)]}
		if err := prepareSharedHeadJob(ctx, e, ids, budget, references); err != nil {
			return err
		}
	}
	return nil
}

func prepareSharedHeadJob(ctx context.Context, e *Engine, ids identitySnapshot, budget, references int64) error {
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
		if budget > 0 {
			if bytes > budget-references {
				return errHeadPlanUnavailable
			}
			return nil
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

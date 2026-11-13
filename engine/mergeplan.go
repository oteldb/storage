package engine

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-faster/sdk/zctx"
)

// MergePlan reports the last numeric merge's memory selection and admission.
type MergePlan struct {
	Candidates, Selected, Attempts   int
	InputResidentBytes, OpenBytes    int64
	ScratchBytes, MetadataBytes      int64
	OutputFloorBytes, AllowanceBytes int64
	// Outcome is selected, admitted, input_memory, pool_busy, completed, or failed.
	Outcome     string
	OutputParts int
}

func (e *Engine) recordMergePlan(ctx context.Context, plan MergePlan, outcome string) {
	plan.Outcome = outcome
	e.lastMergePlan.Store(&plan)
	e.cfg.Obs.Merge.Plan(ctx, metricSignal, outcome, int64(plan.Candidates), int64(plan.Selected),
		plan.InputResidentBytes, plan.OpenBytes, plan.ScratchBytes, plan.MetadataBytes, plan.OutputFloorBytes, plan.AllowanceBytes)
	zctx.From(ctx).Debug("numeric merge plan", zap.String("prefix", e.cfg.Prefix), zap.Any("plan", plan))
}

// LastMergePlan returns a copy without taking an engine or backend lock.
func (e *Engine) LastMergePlan() MergePlan {
	if plan := e.lastMergePlan.Load(); plan != nil {
		return *plan
	}
	return MergePlan{}
}

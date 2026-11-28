package engine

import (
	"context"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/signal"
)

// The part target and the reservation are separate: a small head must not reserve
// a process-sized target, and a smaller reservation must not shrink the run target.
func (b *headPartBuilder) reserveHeadShape(ctx context.Context, samples map[signal.SeriesID]*sampleBuf, order detachedIdentityOrder) error {
	if !backend.StreamsWrites(b.e.cfg.Backend) {
		return nil
	}
	denseBytes := order.view.DenseBudgetBytes(len(samples))
	n := buildbudget.Add(buildbudget.Add(b.baseBytes(), denseBytes), buildbudget.Mul(order.view.EncodingWorkspace(), 2))
	n, err := b.headShapeBytes(ctx, order, samples, n)
	if err != nil {
		return err
	}
	b.reservationFloor = min(b.target, n)
	need := max(b.reservationFloor, buildbudget.Add(b.baseBytes(), denseBytes))
	if need > b.reserved {
		if err := b.reservePhase(need, "shape"); err != nil {
			return err
		}
	}
	if denseBytes > 0 {
		b.identities.UseDenseSymbols(denseBytes)
	}
	return nil
}

func (b *headPartBuilder) headShapeBytes(ctx context.Context, order detachedIdentityOrder,
	samples map[signal.SeriesID]*sampleBuf, base int64,
) (int64, error) {
	total, sorting := base, int64(0)
	for _, run := range order.runs {
		for i, ref := range run {
			if buildbudget.Add(total, sorting) >= b.target {
				return b.target, ctx.Err()
			}
			if i%4096 == 0 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
			buf := order.sampleBuffer(samples, ref)
			if buf == nil || len(buf.ts) == 0 {
				continue
			}
			total = buildbudget.Add(total, buildbudget.Add(512, buildbudget.Mul(order.view.EncodingBytes(int(ref)), 4)))
			sorting = max(sorting, buildbudget.Mul(int64(len(buf.ts)), 48))
		}
	}
	return buildbudget.Add(total, sorting), ctx.Err()
}

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
	n := buildbudget.Add(b.baseBytes(), buildbudget.Mul(order.view.EncodingWorkspace(), 2))
	var sorting int64
	err := walkIdentityOrder(ctx, order.view, order.runs, func(id signal.SeriesID, ref uint32) error {
		buf := samples[id]
		if buf == nil || len(buf.ts) == 0 {
			return nil
		}
		n = buildbudget.Add(n, buildbudget.Add(512, buildbudget.Mul(order.view.EncodingBytes(int(ref)), 4)))
		sorting = max(sorting, buildbudget.Mul(int64(len(buf.ts)), 48))
		return nil
	})
	if err != nil {
		return err
	}
	b.reservationFloor = min(b.target, buildbudget.Add(n, sorting))
	if b.reservationFloor > b.reserved {
		return b.reservePhase(b.reservationFloor, "shape")
	}
	return nil
}

package storage

import (
	"context"

	"go.uber.org/zap"
)

func (s *Storage) mergeMemoryBytes() int64 {
	if s.mergePool == nil {
		return s.opts.MergeMemoryBytes
	}
	return s.mergePool.Total()
}

func (s *Storage) metricFlushMemoryBytes() int64 {
	if s.buildPool == nil {
		return 0
	}
	return max(s.buildPool.Total()/4, 1)
}

func (s *Storage) flushAdmission() func(context.Context, int64) (func(), error) {
	if s.buildPool == nil {
		return nil
	}
	return func(ctx context.Context, bytes int64) (func(), error) {
		return s.buildPool.AcquireObserved(ctx, bytes, s.obs.Memory.BuildFlush)
	}
}

// runPressureFlush owns one bounded pressure pass independently of the merge scheduler.
func (s *Storage) runPressureFlush(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		s.obs.Logger(ctx).Info("maintenance worker stopped", zap.String("worker", "runPressureFlush"), zap.NamedError("context_error", ctx.Err()))
	}()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-s.flushWake:
			if ctx.Err() != nil {
				return
			}
			s.flushPressured(ctx)
			select {
			case s.mergeWake <- struct{}{}:
			default:
			}
		}
	}
}

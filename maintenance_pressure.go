package storage

import "context"

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
	return s.buildPool.Acquire
}

// runPressureFlush owns one bounded pressure pass independently of the merge scheduler.
func (s *Storage) runPressureFlush(ctx context.Context) {
	defer s.wg.Done()
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

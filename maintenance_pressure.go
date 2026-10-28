package storage

import "context"

func (s *Storage) mergeMemoryBytes() int64 {
	if s.mergePool == nil {
		return s.opts.MergeMemoryBytes
	}
	return s.mergePool.Total()
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
		case <-s.flushWake:
			s.flushPressured(ctx)
			select {
			case s.mergeWake <- struct{}{}:
			default:
			}
		}
	}
}

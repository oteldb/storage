package engine

import "context"

// FlushIfIdle flushes the head, skipping it when another build holds flush ownership.
// A skipped flush returns nil; [Engine.Flush] always waits for ownership.
func (e *Engine) FlushIfIdle(ctx context.Context) error {
	return e.observeFlush(ctx, e.flushIfIdle)
}

func (e *Engine) flushIfIdle(ctx context.Context) (int, int64, error) {
	unlock, err := e.admitFlushWorkWith(ctx, true)
	if unlock == nil {
		return 0, 0, err
	}
	defer unlock()
	return e.flushAdmitted(ctx)
}

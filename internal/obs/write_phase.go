package obs

import "context"

type writePhaseKey struct{}

// WithWritePhase marks the storage operation that owns backend writes.
func WithWritePhase(ctx context.Context, phase string) context.Context {
	return context.WithValue(ctx, writePhaseKey{}, phase)
}

// WritePhase returns the owning operation for the backend write counters.
func WritePhase(ctx context.Context) string {
	if phase, ok := ctx.Value(writePhaseKey{}).(string); ok {
		return phase
	}
	return "other"
}

package obs

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type shutdownKey struct{}

// WithShutdown enables lifecycle phase logs for final flushes and resource release.
func WithShutdown(ctx context.Context) context.Context {
	if ctx.Value(shutdownKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, shutdownKey{}, true)
}

// ShutdownPhase logs a phase before work begins and returns its completion callback.
// It is silent outside [WithShutdown] contexts.
func ShutdownPhase(ctx context.Context, logger *zap.Logger, phase string, fields ...zap.Field) func(error) {
	if ctx.Value(shutdownKey{}) == nil {
		return discardShutdown
	}
	started := time.Now()
	logger = logger.With(append(fields, zap.String("phase", phase))...)
	fields = shutdownContextFields(ctx)
	logger.Info("shutdown phase started", fields...)
	return func(err error) {
		fields := append(shutdownContextFields(ctx), zap.Duration("elapsed", time.Since(started)))
		if err != nil {
			logger.Warn("shutdown phase failed", append(fields, zap.Error(err))...)
			return
		}
		logger.Info("shutdown phase completed", fields...)
	}
}

func discardShutdown(error) {}

func shutdownContextFields(ctx context.Context) []zap.Field {
	var fields []zap.Field
	if deadline, ok := ctx.Deadline(); ok {
		fields = append(fields, zap.Time("deadline", deadline), zap.Duration("remaining", time.Until(deadline)))
	}
	if err := ctx.Err(); err != nil {
		fields = append(fields, zap.NamedError("context_error", err))
	}
	return fields
}

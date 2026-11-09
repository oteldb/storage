package obs_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/internal/obs"
)

func TestShutdownPhaseNormalContextIsSilent(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zap.InfoLevel)
	obs.ShutdownPhase(t.Context(), zap.New(core), "flush_write_parts")(nil)
	require.Zero(t, logs.Len())
}

func TestShutdownPhaseReportsFailureAndBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ctx = obs.WithShutdown(ctx)
		require.Same(t, ctx, obs.WithShutdown(ctx))
		finish := obs.ShutdownPhase(ctx, zap.New(core).With(zap.String("prefix", "default/metrics")), "wal_close")
		time.Sleep(2 * time.Second)
		cancel()
		finish(errors.New("sync failed"))
		entries := logs.All()
		require.Len(t, entries, 2)
		require.Equal(t, "shutdown phase started", entries[0].Message)
		require.Equal(t, "shutdown phase failed", entries[1].Message)
		fields := entries[1].ContextMap()
		require.Equal(t, "wal_close", fields["phase"])
		require.Equal(t, "default/metrics", fields["prefix"])
		require.Equal(t, 2*time.Second, fields["elapsed"])
		require.Equal(t, 28*time.Second, fields["remaining"])
		require.Equal(t, context.Canceled.Error(), fields["context_error"])
	})
}

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCloseReportsDurabilityPhases(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zap.InfoLevel)
	store, err := InMemory(WithLogger(zap.New(core)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	_, err = store.WriteMetrics(t.Context(), gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)
	require.NoError(t, store.Close(t.Context()))
	starts := map[string]int{}
	completions := map[string]int{}
	for _, entry := range logs.All() {
		phase, _ := entry.ContextMap()["phase"].(string)
		switch entry.Message {
		case "shutdown phase started":
			starts[phase]++
		case "shutdown phase completed":
			completions[phase]++
		}
	}
	require.NotEmpty(t, starts)
	require.Equal(t, starts, completions)
	for _, phase := range []string{"storage_close", "maintenance_workers", "close_engines", "engine_close", "final_flush", "flush_admission", "flush_write_parts", "flush_publish_checkpoint"} {
		require.Positive(t, starts[phase], phase)
	}
	count := logs.Len()
	require.NoError(t, store.Close(t.Context()))
	require.Equal(t, count, logs.Len(), "repeated Close must remain a no-op")
}

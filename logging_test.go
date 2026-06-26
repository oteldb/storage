package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestLoggingCoarseBoundaries verifies the zap pillar emits at the coarse operation boundaries
// (open, flush, close) when the embedder injects a logger — and stays silent per-sample.
func TestLoggingCoarseBoundaries(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	ctx := context.Background()
	s, err := InMemory(WithLogger(logger))
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	s.maintain(ctx) // force a flush to a part

	require.NoError(t, s.Close(ctx))

	msgs := make(map[string]int)
	for _, e := range logs.All() {
		msgs[e.Message]++
	}

	assert.Equal(t, 1, msgs["storage opened"], "one open line")
	assert.Positive(t, msgs["flushed head to part"], "flush logged at least once")
	assert.Equal(t, 1, msgs["storage closed"], "one close line")

	// The 3-sample write must NOT produce a log line per sample: total lines stay small.
	assert.Less(t, logs.Len(), 20, "logging is coarse, not per-sample")
}

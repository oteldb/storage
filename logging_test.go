package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oteldb/storage/query/fetch"
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

	it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	_, err = fetch.Drain(ctx, it)
	require.NoError(t, err)

	require.NoError(t, s.Close(ctx))

	msgs := make(map[string]int)
	for _, e := range logs.All() {
		msgs[e.Message]++
	}

	// Coarse lifecycle + per-layer debug all flow through the zctx-seeded logger.
	assert.Equal(t, 1, msgs["storage opened"], "one open line")
	assert.Equal(t, 1, msgs["storage closed"], "one close line")
	assert.Positive(t, msgs["write start"], "facade write boundary logged")
	assert.Positive(t, msgs["write done"], "facade write boundary logged")
	assert.Positive(t, msgs["flushed head to part"], "engine flush logged")
	assert.Positive(t, msgs["maintenance cycle start"], "maintenance loop logged")
	assert.Positive(t, msgs["query fetch"], "read boundary logged")
	assert.Positive(t, msgs["fetch start"], "engine fetch logged")
	assert.Positive(t, msgs["fetch done"], "engine fetch logged")

	// The 3-sample write+read must NOT produce a log line per sample: the count is bounded by the
	// number of operations, not the number of rows.
	assert.Less(t, logs.Len(), 60, "logging is coarse, not per-sample")
}

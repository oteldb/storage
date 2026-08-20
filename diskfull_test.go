package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/signal"
)

// TestFullDiskIsVisibleAndRefused is #388 at the facade: an operator must be able to see that a
// node has stopped storing, and a writer must be told rather than acked. Both halves used to be
// silent — the flush failed in the log and the head kept growing.
func TestFullDiskIsVisibleAndRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	full := backendtest.WithCapacity(backend.Memory(), 0, 1<<20)

	s, err := Open(ctx, Options{},
		WithBackend(full),
		WithDurability(DurabilityEphemeral),
		WithDiskReserve(1<<20, 16),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{100}, []float64{1}))
	require.NoError(t, err, "the first write predates any flush: nothing is known to be wrong yet")

	eng, err := s.engineFor("default")
	require.NoError(t, err)
	require.ErrorIs(t, eng.Flush(ctx), backend.ErrNoSpace)

	sig, ok := findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	require.True(t, ok)
	assert.True(t, sig.OutOfSpace, "Inspect is where an operator reads this")
	assert.Equal(t, 0, sig.Parts, "no part was written")
	assert.Positive(t, sig.HeadItems, "the samples are still there to flush once there is room")

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{200}, []float64{2}))
	require.ErrorIs(t, err, backend.ErrNoSpace, "the writer is told, not acked")

	// Reads keep working: the node degrades to read-only, it does not fall over.
	_, err = s.MetricSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)

	full.SetFreeBytes(1 << 30)
	require.NoError(t, eng.Flush(ctx))

	sig, _ = findSignal(mustTenant(t, s.Inspect(), "default"), signal.Metric)
	assert.False(t, sig.OutOfSpace, "the node recovers on its own once the disk is emptied")

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{300}, []float64{3}))
	require.NoError(t, err)
}

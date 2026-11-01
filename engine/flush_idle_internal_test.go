package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestFlushIfIdleOwnership(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"exclusive", "flush-owned", "catalog-owned"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Backend: backend.Memory(), Prefix: "idle"}
			if name != "exclusive" {
				cfg.FlushAdmission = func(context.Context, int64) (func(), error) { return func() {}, nil }
			}
			e := New(cfg)
			_, err := e.Append(apiSeries(), 1, 1)
			require.NoError(t, err)
			var release func()
			if name == "catalog-owned" {
				e.flushMu.Lock()
				release = e.flushMu.Unlock
			} else {
				release = e.lockFlush()
			}
			require.NoError(t, e.FlushIfIdle(t.Context()))
			assert.Zero(t, e.PartCount())
			assert.EqualValues(t, 1, e.Stats().HeadSamples)
			release()
			require.NoError(t, e.FlushIfIdle(t.Context()), "a skipped catalog lock must release flush ownership")
			assert.Equal(t, 1, e.PartCount())
			assert.Zero(t, e.Stats().HeadSamples)
		})
	}
}

func TestFlushIfIdleCancellation(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "canceled"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, e.FlushIfIdle(ctx))
	_, err := e.Append(apiSeries(), 1, 1)
	require.NoError(t, err)
	require.ErrorIs(t, e.FlushIfIdle(ctx), context.Canceled)
	assert.Zero(t, e.PartCount())
	assert.EqualValues(t, 1, e.Stats().HeadSamples)
}

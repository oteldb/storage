package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestFlushThresholdBytesResolution(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		set  int64
		want int64
	}{
		{"zero takes the default", 0, defaultFlushThresholdBytes},
		{"explicit value wins", 4 << 20, 4 << 20},
		{"negative disables the size trigger", -1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := Options{FlushThresholdBytes: tt.set}
			assert.Equal(t, tt.want, o.flushThresholdBytes())
		})
	}
}

// TestFlushThresholdTriggersFlush is the regression test for the size trigger being wired at all:
// with a flush interval far longer than the test could ever wait, writing past the threshold must
// still produce a flushed part (the write path pokes the maintenance loop).
func TestFlushThresholdTriggersFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const threshold = 256 << 10 // 256 KiB of buffered records

	s, err := Open(ctx, Options{},
		WithBackend(backend.Memory()),
		WithFlushInterval(int64(time.Hour)),
		WithFlushThresholdBytes(threshold),
	)
	require.NoError(t, err)

	defer func() { _ = s.Close(ctx) }()

	body := strings.Repeat("x", 1024)

	recs := make([][3]any, 0, 512)
	for i := range 512 {
		recs = append(recs, [3]any{i + 1, 9, body})
	}

	_, err = s.WriteLogs(ctx, logBatch("svc", recs...))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(s.Parts(signal.TenantID("default"), signal.Log)) > 0
	}, 10*time.Second, 10*time.Millisecond, "size trigger did not flush the head")

	assert.Positive(t, s.Inspect().Maintenance.PressureFlushes)
}

// TestFlushThresholdDisabledDoesNotFlush is the other half: with the size trigger off, the same
// write stays in the head until the (long) interval elapses.
func TestFlushThresholdDisabledDoesNotFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s, err := Open(ctx, Options{},
		WithBackend(backend.Memory()),
		WithFlushInterval(int64(time.Hour)),
		WithFlushThresholdBytes(-1),
	)
	require.NoError(t, err)

	defer func() { _ = s.Close(ctx) }()

	body := strings.Repeat("x", 1024)

	recs := make([][3]any, 0, 512)
	for i := range 512 {
		recs = append(recs, [3]any{i + 1, 9, body})
	}

	_, err = s.WriteLogs(ctx, logBatch("svc", recs...))
	require.NoError(t, err)

	assert.Never(t, func() bool {
		return len(s.Parts(signal.TenantID("default"), signal.Log)) > 0
	}, 200*time.Millisecond, 20*time.Millisecond)
}

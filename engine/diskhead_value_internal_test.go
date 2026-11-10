package engine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestDiskHeadDropsValueScratchOnFlush(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "value-scratch", DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("long"), Value: signal.StringValue(bytes.Repeat([]byte("x"), 64<<10))})}
	_, err := e.Append(s, 1, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, cap(e.head.valueScratch), 64<<10)
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, cap(e.head.valueScratch), "flushed identities must not retain their largest temporary value")
}

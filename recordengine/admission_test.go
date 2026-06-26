package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
)

func TestAppendBatchCardinalityLimit(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema})
	lim := recordengine.AppendLimits{MaxSeries: 2}

	r1, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "x"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 1, r1.Accepted)

	r2, err := e.AppendBatch(mkBatch("b", rrec{ts: 1, body: "x"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 1, r2.Accepted)

	// A third distinct stream exceeds the cardinality cap: the whole batch is shed.
	r3, err := e.AppendBatch(mkBatch("c", rrec{ts: 1, body: "x"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 0, r3.Accepted)
	assert.Equal(t, 1, r3.RejectedCardinality)
	assert.Equal(t, 2, e.StreamCount())

	// A known stream is never blocked, even at the cap.
	rb, err := e.AppendBatch(mkBatch("a", rrec{ts: 2, body: "x"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 1, rb.Accepted, "existing stream admitted")
}

func TestAppendBatchInFlightBytesLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})

	// Each record is 16 bytes (ts + sev) + len(body); body "a" ⇒ 17. Cap at two records' worth.
	lim := recordengine.AppendLimits{MaxInFlightBytes: 34}
	r, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "a"}, rrec{ts: 2, body: "a"}, rrec{ts: 3, body: "a"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 2, r.Accepted)
	assert.Equal(t, 1, r.RejectedBytes)
	assert.Equal(t, int64(34), e.HeadBytes())

	// A flush drains the head, reopening the byte valve.
	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, int64(0), e.HeadBytes())

	r2, err := e.AppendBatch(mkBatch("a", rrec{ts: 4, body: "a"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 1, r2.Accepted, "flush reopened the valve")
}

func TestAppendBatchNoLimits(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema})
	r, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "x"}, rrec{ts: 2, body: "y"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 2, r.Accepted)
	assert.Zero(t, r.Rejected())
}

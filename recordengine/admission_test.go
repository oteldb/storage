package recordengine_test

import (
	"context"
	"strings"
	"sync"
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

// blockWrite gates the part-object writes of the first flush on a channel, so a test can hold that
// flush open at the write step while the detached buffers are still resident.
type blockWrite struct {
	backend.Backend

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockWrite) Write(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, "/0000000000/") {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}

	return b.Backend.Write(ctx, key, data)
}

// TestAppendBatchInFlightBytesCountsFlushingHead pins the in-flight measure to the memory that is
// actually resident: a flush detaches the head's buffers, but they (and the flush columns built from
// them) stay alive until the part is published, so HeadBytes and the MaxInFlightBytes valve must keep
// counting them instead of dropping to zero the moment they are moved aside.
func TestAppendBatchInFlightBytesCountsFlushingHead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := &blockWrite{Backend: backend.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
	e := newEngine(t, be)

	// Each record is 16 bytes (ts + sev) + len(body); body "a" ⇒ 17. Cap at two records' worth.
	lim := recordengine.AppendLimits{MaxInFlightBytes: 34}
	r, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "a"}, rrec{ts: 2, body: "a"}), lim)
	require.NoError(t, err)
	require.Equal(t, 2, r.Accepted)

	before := e.HeadBytes()
	require.Equal(t, int64(34), before)

	done := make(chan error, 1)
	go func() { done <- e.Flush(ctx) }()

	<-be.entered // the flush has detached the head and is writing the part

	assert.Equal(t, before, e.HeadBytes(),
		"records detached by an in-flight flush are still resident and must stay in the in-flight measure")

	// The admission consequence: a whole new head must not be admitted on top of the flushing one.
	rd, err := e.AppendBatch(mkBatch("a", rrec{ts: 3, body: "a"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 0, rd.Accepted, "the flushing head still occupies the byte budget")
	assert.Equal(t, 1, rd.RejectedBytes)

	close(be.release)
	require.NoError(t, <-done)

	// Publishing the part releases the detached buffers, reopening the valve.
	assert.Equal(t, int64(0), e.HeadBytes())

	ra, err := e.AppendBatch(mkBatch("a", rrec{ts: 4, body: "a"}), lim)
	require.NoError(t, err)
	assert.Equal(t, 1, ra.Accepted, "publish reopened the valve")
}

// TestInFlightBytesRestoredByFailedFlush covers the abort path: a flush that fails folds the detached
// buffers back into the head, so their bytes must be counted once — as head bytes — not twice.
func TestInFlightBytesRestoredByFailedFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := &rejectWrites{Backend: backend.Memory()}
	e := newEngine(t, be)

	r, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "a"}, rrec{ts: 2, body: "a"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 2, r.Accepted)

	before := e.HeadBytes()

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	assert.Equal(t, before, e.HeadBytes(), "folded-back rows are counted once, not doubled")

	require.NoError(t, e.Flush(ctx))
	assert.Equal(t, int64(0), e.HeadBytes())
}

func TestAppendBatchNoLimits(t *testing.T) {
	t.Parallel()

	e := recordengine.New(recordengine.Config{Schema: testSchema})
	r, err := e.AppendBatch(mkBatch("a", rrec{ts: 1, body: "x"}, rrec{ts: 2, body: "y"}), recordengine.AppendLimits{})
	require.NoError(t, err)
	assert.Equal(t, 2, r.Accepted)
	assert.Zero(t, r.Rejected())
}

package recordengine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestIdentityBytesOutlivesFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	for i := range 100 {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: int64(i), body: "hello"}))
	}

	before := e.Stats()
	require.Positive(t, before.HeadBytes, "records are buffered")
	require.Positive(t, before.IdentityBytes, "stream identity is counted")
	require.EqualValues(t, 100, before.Streams)

	require.NoError(t, e.Flush(ctx))

	after := e.Stats()
	assert.Zero(t, after.HeadBytes, "a flush drains the buffered records")
	assert.Equal(t, before.IdentityBytes, after.IdentityBytes,
		"a flush drains no identity — it outlives the records it named")
	assert.Equal(t, after.IdentityBytes, e.IdentityBytes())
}

func TestIdentityBytesGrowsWithStreams(t *testing.T) {
	t.Parallel()

	const streams = 100

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("svc-0", rrec{ts: 1, body: "a"}))
	one := e.IdentityBytes()

	for i := 1; i < streams; i++ {
		ingest(t, e, mkBatch("svc-"+strconv.Itoa(i), rrec{ts: 1, body: "a"}))
	}

	// Identity tracks the all-time stream count — what churn inflates — so each new stream adds to
	// it. The first stream also pays the tables' fixed cost, so the marginal cost is what is checked.
	all := e.IdentityBytes()
	marginal := (all - one) / (streams - 1)
	assert.Positive(t, marginal)
	assert.Less(t, marginal, int64(4096), "a flat stream identity should not report kilobytes")

	ingest(t, e, mkBatch("svc-0", rrec{ts: 2, body: "b"}))
	assert.Equal(t, all, e.IdentityBytes(), "a repeat stream registers nothing new")
}

package engine

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestBlockSlicedFetchMaterializesNoWholePart is the architectural invariant behind the streaming
// fetch merge: with the block cache enabled and block-framed parts, a fetch resolves every matched
// series straight from cached blocks and never materializes a whole-part decodedPart — so the
// per-fetch transient is the result, not the decoded columns (the RSS cliff that growLen showed).
func TestBlockSlicedFetchMaterializesNoWholePart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := New(Config{Backend: backend.Memory(), Prefix: "t/inv", DecodeCacheBytes: 1 << 20, MetricBlockRows: 4})

	const series, samples = 12, 8 // samples > blockRows ⇒ series span multiple blocks

	for s := range series {
		ser := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
			signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte{byte('a' + s)})},
		)}
		for k := range samples {
			ok, err := e.Append(ser, int64(100+s*1000+k*10), float64(s*10+k))
			require.NoError(t, err)
			require.True(t, ok)
		}
	}

	require.NoError(t, e.Flush(ctx))

	matchers := []fetch.Matcher{{
		Name:  []byte("__name__"),
		Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), []byte("m")) },
	}}
	req := fetch.Request{Start: 0, End: 1 << 40, Matchers: matchers}

	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	ids := e.head.resolve(matchers)
	plan := e.planFetch(ids, req)
	e.mu.RUnlock()

	defer plan.releaseParts()

	require.Len(t, ids, series)
	require.NotEmpty(t, plan.liveParts)
	require.NotEmpty(t, plan.blockReaders, "block-slicing must be active (cache on, blocked part)")

	rows := 0

	for _, id := range ids {
		m, err := plan.mergeSeries(ctx, id)
		require.NoError(t, err)

		ts, _, _ := m.collect(nil, nil)
		rows += len(ts)
	}

	require.Equal(t, series*samples, rows, "every sample is returned")
	require.Empty(t, plan.decoded, "no whole-part decodedPart should be materialized on the block-sliced path")
}

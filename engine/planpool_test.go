package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestPlanPoolReuseNoStaleIdentity guards the recycled plan structures: a broad fetch fills the
// pooled identity slice, and a subsequent narrow fetch reuses it — the slice must be zeroed on reuse
// so the narrow result carries its own identity/samples, not a stale entry from the broad fetch.
func TestPlanPoolReuseNoStaleIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/pool", DecodeCacheBytes: 1 << 20})

	const series = 50

	for s := range series {
		ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
		mustAppend(t, e, ser, 100, float64(s))
		mustAppend(t, e, ser, 110, float64(s)+0.5)
	}

	require.NoError(t, e.Flush(ctx))

	// Broad fetch fills the pooled identity slice to `series` entries.
	broad := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}}
	require.Len(t, fetchAll(t, e, broad), series)

	// Narrow fetch reuses the pooled slice; its one batch must carry host=7's identity and samples,
	// not a leftover from the broad fetch.
	narrow := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("host", "7")}}
	got := fetchAll(t, e, narrow)
	require.Len(t, got, 1)

	v, ok := got[0].Series.Attributes.Get([]byte("host"))
	require.True(t, ok)
	assert.Equal(t, "7", string(v.Str()))
	assert.Equal(t, []float64{7, 7.5}, got[0].Values)

	// Re-running the broad fetch (slice reused again, now grown) must still return all series intact.
	assert.Len(t, fetchAll(t, e, broad), series)
}

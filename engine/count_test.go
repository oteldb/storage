package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestCountPushdown verifies Engine.Count agrees with a full Fetch+drain across head, parts,
// partial windows, and empty matches — the correctness contract the PromQL count() pushdown
// relies on.
func TestCountPushdown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/count"})

	a := mkSeries("__name__", "node_x", "host", "a")
	b := mkSeries("__name__", "node_x", "host", "b")
	c := mkSeries("__name__", "node_y", "host", "c") // different metric, excluded by matcher

	mustAppend(t, e, a, 10, 1)
	mustAppend(t, e, a, 20, 2)
	mustAppend(t, e, b, 15, 3)
	mustAppend(t, e, c, 10, 9) // node_y, won't match

	require.NoError(t, e.Flush(ctx)) // a, b, c → one part

	// More head samples after flush (exercises head + part union).
	mustAppend(t, e, a, 30, 4)
	mustAppend(t, e, b, 35, 5)

	req := fetch.Request{
		Start:    0,
		End:      100,
		Matchers: []fetch.Matcher{eqMatcher("__name__", "node_x")},
	}

	got, err := e.Count(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 2, got, "a and b match node_x; c does not")

	// Window that excludes everything.
	empty, err := e.Count(ctx, fetch.Request{Start: 1000, End: 2000, Matchers: req.Matchers})
	require.NoError(t, err)
	assert.Equal(t, 0, empty, "no samples in [1000,2000]")

	// Window covering only a's part sample (ts=20), not b's (ts=15) — wait, 15 < 20, so [16,20]
	// includes a's ts=20 but not b's ts=15. Head samples (a=30, b=35) are outside too.
	onlyA, err := e.Count(ctx, fetch.Request{Start: 16, End: 20, Matchers: req.Matchers})
	require.NoError(t, err)
	assert.Equal(t, 1, onlyA, "only a has a sample (ts=20) in [16,20]")

	// Agreement with Fetch across a broad window.
	want := len(fetchAll(t, e, req))
	fetchCount, err := e.Count(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, want, fetchCount, "Count must match Fetch cardinality")
}

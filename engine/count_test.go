package engine_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
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

// reMatcher lowers a PromQL-style regex matcher (value matches re) to a fetch.Matcher over the
// typed value's canonical text projection — the same lowering query/promql.PushableMatchers applies
// for `__name__=~"node_.+"` (the full_scan_count query).
func reMatcher(name, pattern string) fetch.Matcher {
	re := regexp.MustCompile(pattern)
	return fetch.Matcher{
		Name:  []byte(name),
		Match: func(v signal.Value) bool { return re.Match(v.AppendText(nil)) },
	}
}

// TestCountPushdownNameRegex reproduces the full_scan_count query — count({__name__=~"node_.+"}) —
// a non-equality (regex) matcher on __name__ that must still be resolved by the count pushdown.
// The benchmark reports this query returning empty from oteldb where the reference returns a row,
// so this test pins the engine-level correctness the PromQL pushdown builds on: Count over a regex
// on __name__ must match the cardinality a full Fetch+drain returns, not zero.
func TestCountPushdownNameRegex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/count-regex"})

	// node_* metrics — the ones `__name__=~"node_.+"` selects.
	cpu := mkSeries("__name__", "node_cpu_seconds_total", "host", "a")
	mem := mkSeries("__name__", "node_memory_MemFree_bytes", "host", "a")
	net := mkSeries("__name__", "node_network_receive_bytes_total", "host", "a")
	// Non-matching metrics — excluded by the regex, present to ensure the value scan doesn't
	// over-count a `node_` prefix or mis-handle a non-node name.
	http := mkSeries("__name__", "http_requests_total", "host", "a")
	up := mkSeries("__name__", "up", "host", "a")

	mustAppend(t, e, cpu, 10, 1)
	mustAppend(t, e, mem, 10, 2)
	mustAppend(t, e, net, 10, 3)
	mustAppend(t, e, http, 10, 4)
	mustAppend(t, e, up, 10, 5)

	require.NoError(t, e.Flush(ctx)) // all → one part

	// Head samples after flush, so the window straddles head + part (the real load shape).
	mustAppend(t, e, cpu, 30, 6)
	mustAppend(t, e, http, 30, 7)

	matcher := reMatcher("__name__", "node_.+")
	req := fetch.Request{Start: 0, End: 100, Matchers: []fetch.Matcher{matcher}}

	got, err := e.Count(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 3, got, "node_cpu/node_mem/node_net match __name__=~node_.+; http/up do not")

	// Cross-check: Count must agree with a full Fetch+drain over the same window/matcher.
	want := len(fetchAll(t, e, req))
	assert.Equal(t, want, got, "Count must match Fetch cardinality for a __name__ regex")

	// A window outside every sample is the empty vector, not zero-counted series.
	empty, err := e.Count(ctx, fetch.Request{Start: 1000, End: 2000, Matchers: req.Matchers})
	require.NoError(t, err)
	assert.Equal(t, 0, empty, "no samples in [1000,2000]")

	// Sanity: a regex that matches nothing resolves to zero too (the postings value scan must
	// not error or fall through to all-series on an unsatisfiable pattern).
	none, err := e.Count(ctx, fetch.Request{Start: 0, End: 100, Matchers: []fetch.Matcher{reMatcher("__name__", "node_zzz")}})
	require.NoError(t, err)
	assert.Equal(t, 0, none, "no series matches __name__=~node_zzz")
}

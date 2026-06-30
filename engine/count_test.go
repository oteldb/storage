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

// TestCountFromIndex exercises the fully-covered-part shortcut: a part whose [minTime, maxTime]
// falls inside the query window contributes its matched series straight from the part index with no
// column decode, while a window-edge part still decodes and binary-searches. The scenario spans
// three parts so a single query hits all three regimes — pruned, fully covered, and partial — and a
// series living in two parts proves cross-part dedup. Count must agree with a full Fetch+drain in
// every window, the same contract that guards the decode path.
func TestCountFromIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/count-index"})

	a := mkSeries("__name__", "node_x", "host", "a")
	b := mkSeries("__name__", "node_x", "host", "b")
	c := mkSeries("__name__", "node_x", "host", "c")
	d := mkSeries("__name__", "node_x", "host", "d")

	// part1 [100,110]: a, b. part2 [200,210]: a (again), c. part3 [300,310]: b (again), d.
	mustAppend(t, e, a, 100, 1)
	mustAppend(t, e, b, 110, 2)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, a, 200, 3)
	mustAppend(t, e, c, 210, 4)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, b, 300, 5)
	mustAppend(t, e, d, 310, 6)
	require.NoError(t, e.Flush(ctx))

	require.Equal(t, 3, e.PartCount(), "three flushes ⇒ three parts, so the multi-part regimes are real")

	matcher := []fetch.Matcher{eqMatcher("__name__", "node_x")}

	// Whole range: all three parts fully covered (intersectMark path); a and b each live in two
	// parts yet count once — distinct series a, b, c, d.
	all := fetch.Request{Start: 0, End: 1000, Matchers: matcher}
	got, err := e.Count(ctx, all)
	require.NoError(t, err)
	assert.Equal(t, 4, got, "a,b,c,d distinct across fully-covered parts (dedup a,b)")
	assert.Equal(t, len(fetchAll(t, e, all)), got, "Count must match Fetch over fully-covered parts")

	// [105,250]: part1 partial (a@100 excluded, b@110 kept → decode path), part2 fully covered
	// (a@200, c@210 → intersectMark), part3 time-pruned (minTime 300 > 250). Distinct: a, b, c.
	mixed := fetch.Request{Start: 105, End: 250, Matchers: matcher}
	gotMixed, err := e.Count(ctx, mixed)
	require.NoError(t, err)
	assert.Equal(t, 3, gotMixed, "b (part1 edge), a & c (part2 covered); d pruned")
	assert.Equal(t, len(fetchAll(t, e, mixed)), gotMixed, "Count must match Fetch across edge+covered+pruned")

	// A window landing strictly between parts covers nothing.
	gap := fetch.Request{Start: 120, End: 190, Matchers: matcher}
	gotGap, err := e.Count(ctx, gap)
	require.NoError(t, err)
	assert.Equal(t, 0, gotGap, "no samples in the inter-part gap")
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

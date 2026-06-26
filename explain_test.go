package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// findNode returns the first node named name in the tree (depth-first), or nil.
func findNode(n *profile.Node, name string) *profile.Node {
	if n == nil {
		return nil
	}

	if n.Name == name {
		return n
	}

	for _, c := range n.Children {
		if got := findNode(c, name); got != nil {
			return got
		}
	}

	return nil
}

func TestExplainAnalyzeFetch(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	s.maintain(ctx) // flush to a part so the scan touches a part

	// Run the fetch with a profile collector installed in ctx.
	pctx, coll := profile.WithCollector(ctx)
	it, err := s.Fetcher("default").Fetch(pctx, fetch.Request{
		Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("m")},
	})
	require.NoError(t, err)
	_, err = fetch.Drain(pctx, it)
	require.NoError(t, err)

	root := coll.Root()
	t.Logf("EXPLAIN ANALYZE:\n%s", root.Render())

	ef := findNode(root, "engine.fetch")
	require.NotNil(t, ef, "tree has an engine.fetch node")
	assert.Equal(t, int64(1), ef.Counters["series_matched"])
	assert.Equal(t, int64(3), ef.Counters["rows"])
	assert.Positive(t, ef.Counters["parts_scanned"])

	require.NotNil(t, findNode(root, "resolve-matchers"), "resolve sub-operator profiled")
	scan := findNode(root, "scan")
	require.NotNil(t, scan, "scan sub-operator profiled")
	assert.Equal(t, int64(3), scan.Counters["rows"])
}

// TestProfileNoCollectorIsNoop confirms a fetch without a collector in ctx still works (the Begin
// calls are no-ops) — the default path.
func TestProfileNoCollectorIsNoop(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)

	it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("m")},
	})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Len(t, batches, 1)
}

func TestExplainAnalyzeFanOut(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("a", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("b", "m", []int64{1}, []float64{2}))
	require.NoError(t, err)

	// A no-tenant (cross-tenant) fetch fans out across both tenants' engines.
	pctx, coll := profile.WithCollector(ctx)
	it, err := s.Fetcher().Fetch(pctx, fetch.Request{Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	_, err = fetch.Drain(pctx, it)
	require.NoError(t, err)

	root := coll.Root()
	t.Logf("EXPLAIN ANALYZE (fan-out):\n%s", root.Render())

	fanout := findNode(root, "fan-out")
	require.NotNil(t, fanout, "cross-tenant fetch has a fan-out node")
	assert.Equal(t, int64(2), fanout.Counters["children"])

	var engineFetches int
	for _, c := range fanout.Children {
		if c.Name == "engine.fetch" {
			engineFetches++
		}
	}
	assert.Equal(t, 2, engineFetches, "one engine.fetch per tenant under the fan-out")
}

func TestExplainAnalyzeRecordSignal(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 1, "x"}))
	require.NoError(t, err)
	s.maintain(ctx)

	pctx, coll := profile.WithCollector(ctx)
	it, err := s.LogFetcher("default").Fetch(pctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)
	_, err = fetch.Drain(pctx, it)
	require.NoError(t, err)

	rf := findNode(coll.Root(), "recordengine.fetch")
	require.NotNil(t, rf, "log fetch profiles the record engine")
	assert.Positive(t, rf.Counters["rows"])
}

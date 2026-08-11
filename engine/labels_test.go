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

// TestLabelNamesAndValues covers the index-driven label metadata: the unmatched (postings-walk) and
// matched (per-identity) paths must agree with what a full fetch would project, across head, parts
// and windows.
func TestLabelNamesAndValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/labels"})

	mustAppend(t, e, mkSeries("__name__", "node_cpu", "host", "a", "mode", "idle"), 100, 1)
	mustAppend(t, e, mkSeries("__name__", "node_cpu", "host", "b", "mode", "user"), 100, 2)
	mustAppend(t, e, mkSeries("__name__", "node_mem", "host", "a"), 100, 3)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, mkSeries("__name__", "node_net", "host", "c"), 500, 4) // head only

	all := fetch.Request{Start: 0, End: 1000}

	names, err := e.LabelNames(ctx, all)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "host", "mode"}, names)

	values, err := e.LabelValues(ctx, all, []byte("__name__"))
	require.NoError(t, err)
	assert.Equal(t, []string{"node_cpu", "node_mem", "node_net"}, values)

	hosts, err := e.LabelValues(ctx, all, []byte("host"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, hosts)

	// Matcher-scoped: only the matched series' values.
	scoped := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("__name__", "node_cpu")}}

	hosts, err = e.LabelValues(ctx, scoped, []byte("host"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, hosts)

	names, err = e.LabelNames(ctx, scoped)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "host", "mode"}, names)

	// A window covering only the head: the flushed part is pruned, so its values are gone.
	headOnly := fetch.Request{Start: 400, End: 600}

	values, err = e.LabelValues(ctx, headOnly, []byte("__name__"))
	require.NoError(t, err)
	assert.Equal(t, []string{"node_net"}, values, "the part is out of the window")

	names, err = e.LabelNames(ctx, headOnly)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "host"}, names, "mode lives only in the pruned part")

	// An unknown name and an empty window resolve to nothing, not an error.
	none, err := e.LabelValues(ctx, all, []byte("nope"))
	require.NoError(t, err)
	assert.Empty(t, none)

	none, err = e.LabelValues(ctx, fetch.Request{Start: 5000, End: 6000}, []byte("host"))
	require.NoError(t, err)
	assert.Empty(t, none)
}

// TestLabelValuesAfterRetention is why the index-driven listing probes liveness: retention drops
// samples and whole parts but never identities (the head's series index is all-time), so a
// postings-only answer would keep offering the values of series whose data is long gone.
func TestLabelValuesAfterRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/labels-retention"})

	mustAppend(t, e, mkSeries("__name__", "node_cpu", "host", "old"), 100, 1)
	mustAppend(t, e, mkSeries("__name__", "node_cpu", "host", "new"), 900, 2)
	require.NoError(t, e.Flush(ctx))

	// Retain only samples at/after 500: the "old" series loses every sample.
	require.NoError(t, e.Merge(ctx, 500))

	all := fetch.Request{Start: 0, End: 10_000}

	hosts, err := e.LabelValues(ctx, all, []byte("host"))
	require.NoError(t, err)
	assert.Equal(t, []string{"new"}, hosts, "the retention-dropped series' value is not offered")

	// The identity is still in the index — the listing filtered it by liveness, not by pruning.
	assert.Equal(t, 2, e.SeriesCount(), "identities outlive retention")

	// And the identity enumeration agrees (it is part-granular over the same live parts).
	series, err := e.Series(ctx, all)
	require.NoError(t, err)
	assert.Equal(t, []string{"new"}, hostsOf(t, series))
}

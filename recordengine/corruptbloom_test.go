package recordengine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
)

// corruptBlooms rewrites every bloom sidecar read into bytes that cannot decode, modeling a store
// that hands back something other than what was written.
func corruptBlooms(be backend.Backend) *faultbackend.Backend {
	return faultbackend.Wrap(be).Add(faultbackend.Rule{
		Kind:    faultbackend.Read,
		Match:   func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/bloom-") },
		Replace: func(_ faultbackend.Op, _ []byte) []byte { return []byte{0xff, 0xff, 0xff, 0xff} },
	})
}

// TestCorruptBloomSidecarIsTolerated is #488: a bloom only ever removes parts the per-row re-check
// would have removed anyway, so a corrupt one must degrade to "not prunable", exactly as a missing
// one does — not take the whole prefix offline.
func TestCorruptBloomSidecarIsTolerated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mem := backend.Memory()

	e := newEngine(t, mem)
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "alpha request", id: "aaaa", attr: [2]string{"tier", "gold"}},
		rrec{ts: 200, body: "beta request", id: "bbbb", attr: [2]string{"tier", "silver"}},
	))
	require.NoError(t, e.Flush(ctx))

	reopened := newEngine(t, corruptBlooms(mem))
	require.NoError(t, reopened.LoadParts(ctx), "a corrupt bloom sidecar must not fail recovery")

	// Every bloom mode: full-text, equality, and the attribute bloom. Without a filter each part
	// survives pruning and the exact predicate still selects the same rows.
	assert.Equal(t, []string{"alpha request"}, bodiesOf(t, reopened, req("api", bodyContains("alpha"))))
	assert.Equal(t, []string{"beta request"}, bodiesOf(t, reopened, req("api", idEquals("bbbb"))))
	assert.Equal(t, []string{"alpha request"}, bodiesOf(t, reopened, req("api", attrEquals("tier", "gold"))))

	// A token no row holds still returns nothing: the fallback widens the scan, never the result.
	assert.Empty(t, bodiesOf(t, reopened, req("api", bodyContains("gamma"))))
}

func bodiesOf(t *testing.T, e *recordengine.Engine, r fetch.Request) []string {
	t.Helper()

	batches := fetchAll(t, e, r)

	out := make([]string, 0, len(batches))
	for _, b := range batches {
		out = append(out, bodies(b)...)
	}

	return out
}

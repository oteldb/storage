package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// TestSharedBackendFlushKeepsPeerPart is the record engine's copy of the metric engine's
// shared-store reproducer: two engines over one backend under one prefix, as every replica of a
// shard runs in the shared-store deployment (cluster.Config.PrivateBackend false). Both flushes
// report success and only one survives — the second mints a sequence the first already used and
// then commits an index naming only its own parts. See #392 and #383.
func TestSharedBackendFlushKeepsPeerPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	a := newEngine(t, be)
	require.NoError(t, a.LoadParts(ctx))
	b := newEngine(t, be)
	require.NoError(t, b.LoadParts(ctx))

	ingest(t, a, mkBatch("api", rrec{ts: 100, body: "from-a"}))
	ingest(t, b, mkBatch("web", rrec{ts: 200, body: "from-b"}))

	require.NoError(t, a.Flush(ctx))
	require.NoError(t, b.Flush(ctx))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))

	var got []string
	for _, svc := range []string{"api", "web"} {
		for _, batch := range fetchAll(t, r, req(svc)) {
			got = append(got, bodies(batch)...)
		}
	}

	require.ElementsMatch(t, []string{"from-a", "from-b"}, got,
		"a flush that reported success must not drop another engine's committed part")
}

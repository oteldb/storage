package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

// mirror copies every object of src into a fresh backend — what a shared-nothing replica's part sync
// does, frozen at the moment it is called, so a later flush on the owner leaves the copy behind.
func mirror(t *testing.T, src backend.Backend) backend.Backend {
	t.Helper()

	ctx := context.Background()
	dst := backend.Memory()

	keys, err := src.List(ctx, "")
	require.NoError(t, err)

	for _, k := range keys {
		data, err := src.Read(ctx, k)
		require.NoError(t, err)
		require.NoError(t, dst.Write(ctx, k, data))
	}

	return dst
}

// TestRefreshReplicaKeepsHeadTheLaggingPartsMissed pins what makes a shared-nothing replica safe to
// read while its part mirror is behind: the refresh trims the head against the parts *this node
// actually has*, never against the owner's newer set. A replica whose mirror stopped after the first
// flush still answers with everything — the older rows from its part, the newer ones from the head
// the owner has since flushed but not yet handed over.
func TestRefreshReplicaKeepsHeadTheLaggingPartsMissed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	api := mkSeries("job", "api")
	id := api.Hash()

	// The owner flushes once, and the replica mirrors that state.
	owner := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	mustAppend(t, owner, api, 100, 1)
	require.NoError(t, owner.Flush(ctx))

	lagging := mirror(t, be)

	// The owner takes more writes and flushes again. That second part exists only on the owner's
	// backend: the replica's mirror is stalled.
	mustAppend(t, owner, api, 200, 2)
	require.NoError(t, owner.Flush(ctx))

	// The replica holds both samples in its head, replicated from the primary as they were written.
	replica := engine.New(engine.Config{Backend: lagging, Prefix: "default/metrics"})
	require.NoError(t, replica.ApplyReplicated(walOf(t, func(w *wal.Writer) {
		require.NoError(t, w.WriteSeries(id, api))
		require.NoError(t, w.WriteSamples(id, []int64{100, 200}, []float64{1, 2}))
	})))

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 1, replica.PartCount(), "the mirror is still one flush behind the owner")

	got := fetchAll(t, replica, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps,
		"the sample the lagging mirror has no part for is still served from the head")
	assert.Equal(t, []float64{1, 2}, got[0].Values)
}

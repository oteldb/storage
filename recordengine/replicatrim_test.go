package recordengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// flushedApartFromEachOther seeds a backend with two parts: one holding "api" up to t=100, a later
// one holding only "web" at t=100000. The part set's newest time therefore belongs to a stream that
// says nothing about api's durability — the shape the replica trim has to tell apart.
func flushedApartFromEachOther(t *testing.T, be backend.Backend) {
	t.Helper()

	ctx := context.Background()
	owner := newEngine(t, be)

	ingest(t, owner, mkBatch("api", rrec{ts: 100, body: "old"}))
	require.NoError(t, owner.Flush(ctx))

	ingest(t, owner, mkBatch("web", rrec{ts: 100000, body: "new"}))
	require.NoError(t, owner.Flush(ctx))
}

// TestRefreshReplicaTrimsPerStream: the replica head is trimmed against each stream's own newest
// flushed record, not the newest across the part set. A record for a stream whose own flushed data
// is older survives the refresh; one already durable for its own stream does not.
func TestRefreshReplicaTrimsPerStream(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	flushedApartFromEachOther(t, be)

	replica := newEngine(t, be)
	// Replicated, acked, and not yet flushed by the owner: legal under api's own lateness bound
	// (its newest flushed record is t=100), and older than what web has flushed.
	ingest(t, replica, mkBatch("api", rrec{ts: 500, body: "late"}))
	// Already durable in web's own part — the trim must still drop this one.
	ingest(t, replica, mkBatch("web", rrec{ts: 100000, body: "new"}))
	require.Equal(t, 2, replica.HeadRecordCount())

	require.NoError(t, replica.RefreshReplica(ctx))
	require.Equal(t, 2, replica.PartCount())
	assert.Equal(t, 1, replica.HeadRecordCount(),
		"web's flushed record is dropped, api's unflushed one is kept")

	got := fetchAll(t, replica, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 500}, got[0].Timestamps,
		"the late record another stream's flush does not cover is still served")
	assert.Equal(t, []string{"old", "late"}, bodies(got[0]))

	web := fetchAll(t, replica, req("web"))
	require.Len(t, web, 1)
	assert.Equal(t, []int64{100000}, web[0].Timestamps, "the trimmed record is served from the part, once")
}

// TestPromotedReplicaKeepsLateRecord is the loss case: a replica that trimmed a late record and is
// then promoted before the owner's next flush has nowhere to recover it from. Promotion is modeled
// by the replica flushing its own head — the promoted node's first flush — and a fresh reader
// loading the resulting parts.
func TestPromotedReplicaKeepsLateRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	flushedApartFromEachOther(t, be)

	replica := newEngine(t, be)
	ingest(t, replica, mkBatch("api", rrec{ts: 500, body: "late"}))
	require.NoError(t, replica.RefreshReplica(ctx))

	require.NoError(t, replica.Flush(ctx))

	promoted := newEngine(t, be)
	require.NoError(t, promoted.LoadParts(ctx))

	got := fetchAll(t, promoted, req("api"))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 500}, got[0].Timestamps,
		"the record the promoted node flushed is durable, not lost with the old owner's head")
}

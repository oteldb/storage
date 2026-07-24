package recordengine_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

// rejectReads wraps a backend and fails Read for keys with a given suffix while armed. It aborts a
// flush *after* the part's objects are fully written (at the openPart read-back), which is what
// leaves an orphan part behind.
type rejectReads struct {
	backend.Backend

	armed atomic.Bool
	only  string
}

func (r *rejectReads) Read(ctx context.Context, key string) ([]byte, error) {
	if r.armed.Load() && strings.HasSuffix(key, r.only) {
		return nil, errors.New("injected read failure")
	}

	return r.Backend.Read(ctx, key)
}

// partObjects returns the backend keys under the engine prefix that belong to the part with the given
// sequence.
func partObjects(t *testing.T, be backend.Backend, seq string) []string {
	t.Helper()

	keys, err := be.List(context.Background(), "t/recs/"+seq+"/")
	require.NoError(t, err)

	return keys
}

// TestFailedFlushBurnsPartSequence verifies a flush that wrote part objects and then failed does not
// hand its sequence to the retry: reusing the prefix would overwrite only the objects the retry itself
// produces, and two of a part's objects are conditional (the record-key footer, the side-store
// sidecars), so the new part would adopt the failed attempt's.
func TestFailedFlushBurnsPartSequence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := newEngine(t, be)

	// Attempt 1: the part's objects (columns, blooms, keys.bin) are written; the openPart read-back
	// fails, so it is never published and its objects are orphaned at sequence 0.
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "orphan", attr: [2]string{"http.method", "GET"}}))
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	require.NotEmpty(t, partObjects(t, be, "0000000000"), "the failed attempt's objects are still there")

	// The retry (the rows are folded back into the head, so it re-flushes them) takes a fresh sequence.
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 1, e.PartCount())
	require.NotEmpty(t, partObjects(t, be, "0000000001"), "the retry must write to a fresh sequence")
	require.Equal(t, []string{"orphan"}, streamBodies(t, e))
}

// TestLoadPartsSweepsOrphanParts verifies the open-time sweep: the objects of a part the bucket index
// does not name are deleted, and the sequence resumes past them even across a restart (where the
// index alone would hand the orphan's sequence back out).
func TestLoadPartsSweepsOrphanParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "orphan", attr: [2]string{"http.method", "GET"}}))
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)
	require.NotEmpty(t, partObjects(t, be, "0000000000"))

	// Restart: the index names no part, so the orphan is swept and its sequence is not reused.
	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Empty(t, partObjects(t, be, "0000000000"), "an orphan part's objects must be swept at open")

	ingest(t, r, mkBatch("api", rrec{ts: 200, body: "live"}))
	require.NoError(t, r.Flush(ctx))

	require.NotEmpty(t, partObjects(t, be, "0000000001"), "the sequence must resume past the orphan")
	require.NotContains(t, keyScopes(r.Keys(0, 1<<60)), "http.method")
	require.Equal(t, []string{"live"}, streamBodies(t, r))
}

// TestLoadPartsKeepsLiveParts guards the sweep against deleting the parts the index does name.
func TestLoadPartsKeepsLiveParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))

	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))

	require.Equal(t, 2, r.PartCount())
	require.Equal(t, []string{"p1", "p2"}, streamBodies(t, r))
}

// TestRefreshReplicaKeepsUncommittedParts verifies a replica's refresh does not sweep: it shares the
// prefix with the owner, whose freshly written but not yet committed part must survive.
func TestRefreshReplicaKeepsUncommittedParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "uncommitted"}))
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	replica := newEngine(t, be)
	require.NoError(t, replica.RefreshReplica(ctx))

	require.NotEmpty(t, partObjects(t, be, "0000000000"),
		"a replica must not delete part objects the owner may still be committing")
}

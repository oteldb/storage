package recordengine_test

import (
	"context"
	"maps"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/partid"
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
// id.
func partObjects(t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(context.Background(), "t/recs/"+id+"/")
	require.NoError(t, err)

	return keys
}

// isPartObject reports whether key names an object inside a part directory rather than an
// engine-level object (the bucket index, the stream index).
func isPartObject(key string) bool {
	return slices.ContainsFunc(strings.Split(path.Dir(key), "/"), partid.Valid)
}

// partDirs returns the sorted part ids that have objects under the engine prefix — part ids are
// minted, so a test cannot name them up front and asserts over this set instead.
func partDirs(t *testing.T, be backend.Backend) []string {
	t.Helper()

	keys, err := be.List(context.Background(), "t/recs/")
	require.NoError(t, err)

	seen := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		dir, _, ok := strings.Cut(strings.TrimPrefix(k, "t/recs/"), "/")
		if ok && partid.Valid(dir) {
			seen[dir] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(seen))
}

// TestFailedFlushBurnsPartID verifies a flush that wrote part objects and then failed does not hand
// its id to the retry: reusing the prefix would overwrite only the objects the retry itself
// produces, and two of a part's objects are conditional (the record-key footer, the side-store
// sidecars), so the new part would adopt the failed attempt's.
func TestFailedFlushBurnsPartID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := newEngine(t, be)

	// Attempt 1: the part's objects (columns, blooms, keys.bin) are written; the openPart read-back
	// fails, so it is never published and its objects are orphaned under its id.
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "orphan", attr: [2]string{"http.method", "GET"}}))
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	orphans := partDirs(t, be)
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]), "the failed attempt's objects are still there")

	// The retry (the rows are folded back into the head, so it re-flushes them) takes a fresh id.
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 1, e.PartCount())

	dirs := partDirs(t, be)
	require.Len(t, dirs, 2, "the retry must write to a fresh id, leaving the burnt one behind")
	require.Contains(t, dirs, orphans[0])
	require.Equal(t, []string{"orphan"}, streamBodies(t, e))
}

// TestLoadPartsSweepsOrphanParts verifies the open-time sweep: the objects of a part the bucket index
// does not name are deleted, and a part written after the restart lands on a fresh id rather than on
// the orphan's.
func TestLoadPartsSweepsOrphanParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "orphan", attr: [2]string{"http.method", "GET"}}))
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	orphans := partDirs(t, be)
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]))

	// Restart: the index names no part, so the orphan is swept and its id is not reused.
	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Empty(t, partObjects(t, be, orphans[0]), "an orphan part's objects must be swept at open")

	ingest(t, r, mkBatch("api", rrec{ts: 200, body: "live"}))
	require.NoError(t, r.Flush(ctx))

	dirs := partDirs(t, be)
	require.Len(t, dirs, 1)
	require.NotEqual(t, orphans[0], dirs[0], "the new part must not land on the orphan's id")
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

	orphans := partDirs(t, be)
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]),
		"a replica must not delete part objects the owner may still be committing")
}

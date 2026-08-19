package engine_test

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
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/query/fetch"
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

// partObjects returns the backend keys belonging to the part with the given id.
func partObjects(t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(context.Background(), "default/metrics/"+id+"/")
	require.NoError(t, err)

	return keys
}

// isPartObject reports whether key names an object inside a part directory rather than an
// engine-level object (the bucket index, the legacy series set).
func isPartObject(key string) bool {
	return slices.ContainsFunc(strings.Split(path.Dir(key), "/"), partid.Valid)
}

// partDirs returns the sorted part ids that have objects under the engine prefix — part ids are
// minted, so a test cannot name them up front and asserts over this set instead.
func partDirs(t *testing.T, be backend.Backend, enginePrefix string) []string {
	t.Helper()

	keys, err := be.List(context.Background(), enginePrefix+"/")
	require.NoError(t, err)

	seen := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		dir, _, ok := strings.Cut(strings.TrimPrefix(k, enginePrefix+"/"), "/")
		if ok && partid.Valid(dir) {
			seen[dir] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(seen))
}

// TestFailedFlushBurnsPartID verifies a flush that wrote part objects and then failed does not hand
// its id to the retry: a rewrite replaces only the objects it itself produces, so a part landing on
// the leftovers can inherit objects it never wrote.
func TestFailedFlushBurnsPartID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)

	// The part's objects are written; the openPart read-back fails, so it is never published.
	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	orphans := partDirs(t, be, "default/metrics")
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]), "the failed attempt's objects are still there")
	require.Equal(t, 0, e.PartCount())

	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	require.Equal(t, 1, e.PartCount())

	dirs := partDirs(t, be, "default/metrics")
	require.Len(t, dirs, 2, "the retry must write to a fresh id, leaving the burnt one behind")
	require.Contains(t, dirs, orphans[0])
}

// TestLoadPartsSweepsOrphanParts verifies the open-time sweep: the objects of a part the bucket index
// does not name are deleted, and a part written after the restart lands on a fresh id rather than on
// the orphan's.
func TestLoadPartsSweepsOrphanParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	orphans := partDirs(t, be, "default/metrics")
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]))

	// Restart: the index names no part, so the orphan is swept and its id is not reused.
	r := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	require.NoError(t, r.LoadParts(ctx))
	require.Empty(t, partObjects(t, be, orphans[0]), "an orphan part's objects must be swept at open")

	mustAppend(t, r, s, 200, 2.0)
	require.NoError(t, r.Flush(ctx))

	dirs := partDirs(t, be, "default/metrics")
	require.Len(t, dirs, 1)
	require.NotEqual(t, orphans[0], dirs[0], "the new part must not land on the orphan's id")

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Equal(t, []int64{200}, got[0].Timestamps)
}

// TestLoadPartsKeepsLiveParts guards the sweep against deleting the parts the index does name.
func TestLoadPartsKeepsLiveParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(ctx))

	r := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	require.NoError(t, r.LoadParts(ctx))

	require.Equal(t, 2, r.PartCount())

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Equal(t, []int64{100, 200}, got[0].Timestamps)
}

// TestRefreshReplicaKeepsUncommittedParts verifies a replica's refresh does not sweep: it shares the
// prefix with the owner, whose freshly written but not yet committed part must survive.
func TestRefreshReplicaKeepsUncommittedParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectReads{Backend: backend.Memory(), only: "/manifest"}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	be.armed.Store(true)
	require.Error(t, e.Flush(ctx))
	be.armed.Store(false)

	replica := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	require.NoError(t, replica.RefreshReplica(ctx))

	orphans := partDirs(t, be, "default/metrics")
	require.Len(t, orphans, 1)
	require.NotEmpty(t, partObjects(t, be, orphans[0]),
		"a replica must not delete part objects the owner may still be committing")
}

package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/obs/obstest"
)

var orphanRow = Row{Stream: apiStream, Ts: 100, Val: 1, Attr: [2]string{"http.method", "GET"}}

// failOrphanFlush flushes orphanRow while the openPart read-back fails, so the part's objects are
// written but it is never published, and returns the orphan's id.
func (k Kind) failOrphanFlush(ctx context.Context, t *testing.T, e Engine, be *faultbackend.Backend) string {
	t.Helper()

	e.Append(t, orphanRow)

	rejectReads(be, "/manifest", errReadRejected)
	require.Error(t, e.Flush(ctx))
	be.Reset()

	orphans := k.partDirs(ctx, t, be)
	require.Len(t, orphans, 1)
	require.NotEmpty(t, k.partKeys(ctx, t, be, orphans[0]), "the failed attempt's objects are still there")

	return orphans[0]
}

// failedFlushBurnsPartID: a flush that wrote part objects and then failed does not hand its id to
// the retry. A rewrite replaces only the objects it produces, so a part landing on the leftovers
// would inherit objects it never wrote (for records: the conditional key footer and side-store
// sidecars).
func failedFlushBurnsPartID(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)

	orphan := k.failOrphanFlush(ctx, t, e, be)
	require.Equal(t, 0, e.PartCount())

	// The failed flush folded its rows back into the head, so the retry flushes them too.
	e.Append(t, api(200, 2))
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 1, e.PartCount())

	dirs := k.partDirs(ctx, t, be)
	require.Len(t, dirs, 2, "the retry must write to a fresh id, leaving the burnt one behind")
	require.Contains(t, dirs, orphan)
	assert.ElementsMatch(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream))
}

// loadPartsSweepsOrphanParts: the objects of a part the bucket index does not name are deleted at
// open, and a part written after the restart lands on a fresh id rather than on the orphan's.
func loadPartsSweepsOrphanParts(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	orphan := k.failOrphanFlush(ctx, t, k.open(t, be), be)
	objects := len(k.partKeys(ctx, t, be, orphan))

	o, m := obstest.New(t)
	r := k.Open(t, Config{Backend: be, Obs: o})
	require.NoError(t, r.LoadParts(ctx))
	require.Empty(t, k.partKeys(ctx, t, be, orphan), "an orphan part's objects must be swept at open")
	require.Equal(t, int64(objects), m.Counter("storage.parts.orphans_swept"))

	r.Append(t, api(200, 2))
	require.NoError(t, r.Flush(ctx))

	dirs := k.partDirs(ctx, t, be)
	require.Len(t, dirs, 1)
	require.NotEqual(t, orphan, dirs[0], "the new part must not land on the orphan's id")
	require.NotContains(t, r.AttrNames(t), orphanRow.Attr[0])
	require.Equal(t, []Row{api(200, 2)}, rows(t, r, apiStream))
}

// loadPartsKeepsLiveParts guards the sweep against deleting the parts the index does name.
func loadPartsKeepsLiveParts(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	k.twoParts(t, k.open(t, be), be)

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, 2, r.PartCount())
	require.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, r, apiStream))
}

// refreshReplicaKeepsUncommittedParts: a replica's refresh does not sweep. It shares the prefix with
// the owner, whose freshly written but not yet committed part must survive.
func refreshReplicaKeepsUncommittedParts(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	orphan := k.failOrphanFlush(ctx, t, k.open(t, be), be)

	require.NoError(t, k.open(t, be).RefreshReplica(ctx))
	require.Equal(t, []string{orphan}, k.partDirs(ctx, t, be))
	require.NotEmpty(t, k.partKeys(ctx, t, be, orphan),
		"a replica must not delete part objects the owner may still be committing")
}

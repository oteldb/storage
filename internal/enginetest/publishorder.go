package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// publishCommitsBucketIndexLast: the bucket index is what makes a part durably visible, so it is
// written after everything that part needs to stay readable, including its identity object. A
// stateless reader resolves a part's rows through the identities the part carries: without them the
// rows would be listed but unreachable forever. With no WAL, that inconsistency is unrecoverable.
func publishCommitsBucketIndexLast(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	w := k.open(t, be)
	w.Append(t, api(100, 1))

	rejectWrites(be, "/identity", errWriteRejected)
	require.Error(t, w.Flush(ctx), "flush must fail while the part's identities cannot be written")
	be.Reset()

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	require.Zero(t, r.StreamCount(), "the identity write failed, so no identity is durable")
	require.Zero(t, r.PartCount(),
		"nothing may be committed once the identity write failed: the bucket index is the commit point")
}

// uncommittedPartIdentityIsNotLoaded is the other half: the identity object lands but the bucket
// index does not, so the part was never committed. Identity is scoped to the part, so the orphan
// carries its identities with it and recovery loads neither.
func uncommittedPartIdentityIsNotLoaded(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())

	w := k.open(t, be)
	w.Append(t, api(100, 1))

	rejectWrites(be, "/"+bucketindex.Object, errWriteRejected)
	require.Error(t, w.Flush(ctx))
	be.Reset()

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))
	require.Zero(t, r.PartCount(), "the part was never committed")
	require.Zero(t, r.StreamCount(), "its identities were never loaded — they live with the part")
	assert.Empty(t, rows(t, r, apiStream), "nothing resolves to the uncommitted part")
}

// publishWritesIdentityBeforeCommit: with a WAL, the bucket index also carries the flush watermark.
// A flush whose identity write fails must not commit, or replay would skip rows whose identities
// are missing, leaving them unreachable forever.
func publishWritesIdentityBeforeCommit(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	dir := t.TempDir()

	w := createWAL(t, dir)
	e := k.Open(t, Config{Backend: be, WAL: w})

	e.Append(t, api(100, 1))
	require.NoError(t, w.Sync())

	rejectWrites(be, "/identity", errWriteRejected)
	require.Error(t, e.Flush(ctx), "flush must fail while the part's identities cannot be written")
	be.Reset()

	r := k.Open(t, Config{Backend: be, WAL: createWAL(t, dir)})
	require.NoError(t, r.LoadParts(ctx))
	require.NoError(t, r.Replay(t.Context(), dir))

	require.Equal(t, []Row{api(100, 1)}, rows(t, r, apiStream),
		"rows must stay reachable after a crash between the identity object and the bucket index")
}

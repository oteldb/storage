package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// mergeIndexCommitFailureKeepsSources: a merge whose index commit fails does not retire its source
// parts. The persisted index still names them, so reclaim must not delete their objects, or a
// restart's LoadParts hard-fails on a part the index says exists.
func mergeIndexCommitFailureKeepsSources(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	e := k.open(t, be)
	want := []Row{api(100, 1), api(200, 2), api(300, 3)}

	k.flushEach(t, e, be, want...)
	require.Equal(t, 3, e.PartCount())

	// The compacted part is written, but the index commit fails: the merge must roll back to the
	// committed part set instead of publishing one that is not durable.
	rejectWrites(be, "/"+bucketindex.Object, errWriteRejected)
	require.Error(t, e.Merge(ctx, 0))
	be.Reset()

	require.Equal(t, 3, e.PartCount(), "the uncommitted merge output must not be observable as published")
	require.Equal(t, want, rows(t, e, apiStream), "the source parts stay readable")

	// A later cycle sweeps retired parts; nothing the persisted index names may go with them.
	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, want, rows(t, e, apiStream))

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx), "reclaim must not delete parts the persisted index still lists")
	require.Equal(t, want, rows(t, r, apiStream))
}

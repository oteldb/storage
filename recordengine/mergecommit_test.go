package recordengine_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// rejectIndexWrites wraps a backend and fails only the bucket index write while armed: a merge that
// writes its compacted part fine but cannot commit the part set.
type rejectIndexWrites struct {
	backend.Backend

	armed atomic.Bool
}

func (r *rejectIndexWrites) Write(ctx context.Context, key string, data []byte) error {
	if r.armed.Load() && strings.HasSuffix(key, "/"+bucketindex.Object) {
		return errors.New("injected write failure")
	}

	return r.Backend.Write(ctx, key, data)
}

// CompareAndSwap is the path the index commit actually takes; failing only Write would leave the
// commit untouched.
func (r *rejectIndexWrites) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	if r.armed.Load() && strings.HasSuffix(key, "/"+bucketindex.Object) {
		return backend.VersionAbsent, false, errors.New("injected write failure")
	}

	return r.Backend.CompareAndSwap(ctx, key, expected, data)
}

// TestMergeIndexCommitFailureKeepsSources verifies a merge whose index commit fails does not retire
// its source parts: the persisted index still names them, so reclaim must not delete their objects —
// otherwise a restart's LoadParts hard-fails on a part the index says exists.
func TestMergeIndexCommitFailureKeepsSources(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectIndexWrites{Backend: backend.Memory()}
	e := newEngine(t, be)

	// Two parts, so a merge has something to compact.
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 2, e.PartCount())

	// The compacted part is written, but the index commit fails: the merge must roll back to the
	// committed part set instead of publishing one that is not durable.
	be.armed.Store(true)
	require.Error(t, e.Merge(ctx, 0))
	be.armed.Store(false)

	require.Equal(t, 2, e.PartCount(), "the uncommitted merge output must not be observable as published")
	require.Equal(t, []string{"p1", "p2"}, streamBodies(t, e), "the source parts stay readable")

	// A later cycle sweeps retired parts; nothing the persisted index names may go with them.
	require.NoError(t, e.Merge(ctx, 0))
	require.Equal(t, []string{"p1", "p2"}, streamBodies(t, e))

	// A restart reconstructs from the persisted index.
	r := newEngine(t, be)
	require.NoError(t, r.LoadParts(ctx), "reclaim must not delete parts the persisted index still lists")
	require.Equal(t, []string{"p1", "p2"}, streamBodies(t, r))
}

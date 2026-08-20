package engine_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
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

// seriesSamples returns the timestamps and values the engine holds for the "api" series.
func seriesSamples(t *testing.T, e *engine.Engine) ([]int64, []float64) {
	t.Helper()

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)

	return got[0].Timestamps, got[0].Values
}

// TestMergeIndexCommitFailureKeepsSources verifies a merge whose index commit fails does not retire
// its source parts: the persisted index still names them, so reclaim must not delete their objects —
// otherwise a restart's LoadParts hard-fails on a part the index says exists.
func TestMergeIndexCommitFailureKeepsSources(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectIndexWrites{Backend: backend.Memory()}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	s := mkSeries("job", "api")

	// Three parts, so a merge has something to compact.
	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 300, 3.0)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 3, e.PartCount())

	// The compacted part is written, but the index commit fails: the merge must roll back to the
	// committed part set instead of publishing one that is not durable.
	be.armed.Store(true)
	require.Error(t, e.Merge(ctx, 0))
	be.armed.Store(false)

	require.Equal(t, 3, e.PartCount(), "the uncommitted merge output must not be observable as published")

	ts, vals := seriesSamples(t, e)
	require.Equal(t, []int64{100, 200, 300}, ts, "the source parts stay readable")
	require.Equal(t, []float64{1, 2, 3}, vals)

	// A later cycle sweeps retired parts; nothing the persisted index names may go with them.
	require.NoError(t, e.Merge(ctx, 0))

	ts, vals = seriesSamples(t, e)
	require.Equal(t, []int64{100, 200, 300}, ts)
	require.Equal(t, []float64{1, 2, 3}, vals)

	// A restart reconstructs from the persisted index.
	r := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})
	require.NoError(t, r.LoadParts(ctx), "reclaim must not delete parts the persisted index still lists")

	ts, vals = seriesSamples(t, r)
	require.Equal(t, []int64{100, 200, 300}, ts)
	require.Equal(t, []float64{1, 2, 3}, vals)
}

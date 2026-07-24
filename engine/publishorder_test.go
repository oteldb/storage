package engine_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// rejectWrites wraps a backend and fails Write while armed. A non-empty only restricts the failure
// to keys with that suffix, which models a crash at one specific point of the publish sequence.
type rejectWrites struct {
	backend.Backend

	armed atomic.Bool
	only  string
}

func (r *rejectWrites) Write(ctx context.Context, key string, data []byte) error {
	if r.armed.Load() && (r.only == "" || strings.HasSuffix(key, r.only)) {
		return errors.New("injected write failure")
	}

	return r.Backend.Write(ctx, key, data)
}

// TestPublishCommitsBucketIndexLast verifies the publish ordering: the bucket index is what makes a
// part durably visible, so it must be written after everything that part needs to stay readable —
// here the series identity index. A crash between the two must not leave a committed part behind,
// because a stateless reader rebuilds identities from series.bin alone and cannot resolve a series
// that never made it there: the part's samples would be listed but unreachable forever.
//
// The engine is configured without a WAL (a persistent backend and no WALDir — flushed parts are
// durable, the unflushed head is not), which is where the inconsistent commit is unrecoverable.
func TestPublishCommitsBucketIndexLast(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory(), only: "/series.bin"}
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}

	w := engine.New(cfg)
	mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)

	be.armed.Store(true)
	require.Error(t, w.Flush(ctx), "flush must fail while the series index cannot be written")
	be.armed.Store(false)

	// A fresh reader reconstructs purely from the object store.
	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))

	require.Zero(t, r.SeriesCount(), "the series index write failed, so no identity is durable")
	require.Zero(t, r.PartCount(),
		"nothing may be committed once the series index failed: the bucket index is the commit point")
}

// TestPublishedSeriesWithoutPartIsInert verifies the property that makes the ordering safe: the
// series index may be a superset of what the committed parts hold. An identity persisted whose part
// never committed resolves to a series id no part range covers, so it contributes no batch — the
// reverse (rows with no identity) is the unrecoverable direction.
func TestPublishedSeriesWithoutPartIsInert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectWrites{Backend: backend.Memory(), only: "/" + bucketindex.Object}
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}

	w := engine.New(cfg)
	mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)

	// series.bin lands, the bucket index does not: the durable state carries an identity with no part.
	be.armed.Store(true)
	require.Error(t, w.Flush(ctx))
	be.armed.Store(false)

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, 1, r.SeriesCount(), "the identity is durable")
	require.Zero(t, r.PartCount(), "its part was never committed")

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	assert.Empty(t, got, "a resolvable identity with no rows behind it yields no batch")
}

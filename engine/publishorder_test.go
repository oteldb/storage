package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

var errWriteRejected = errors.New("injected write failure")

// rejectWrites fails every Write and CompareAndSwap of a key ending in suffix with err; an empty
// suffix matches every key. CompareAndSwap is the path the bucket-index commit takes, so a suffix
// naming the index must reject it too.
func rejectWrites(be *faultbackend.Backend, suffix string, err error) {
	match := func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, suffix) }
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Match: match, Err: err})
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Match: match, Err: err})
}

// TestPublishCommitsBucketIndexLast verifies the publish ordering: the bucket index is what makes a
// part durably visible, so it must be written after everything that part needs to stay readable —
// including the part's own identity object. A crash between the two must not leave a committed part
// behind, because a stateless reader resolves the part's rows through the identities the part
// carries: without them the samples would be listed but unreachable forever.
//
// The engine is configured without a WAL (a persistent backend and no WALDir — flushed parts are
// durable, the unflushed head is not), which is where the inconsistent commit is unrecoverable.
func TestPublishCommitsBucketIndexLast(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}

	w := engine.New(cfg)
	mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)

	rejectWrites(be, "/identity", errWriteRejected)
	require.Error(t, w.Flush(ctx), "flush must fail while the part's identities cannot be written")
	be.Reset()

	// A fresh reader reconstructs purely from the object store.
	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))

	require.Zero(t, r.SeriesCount(), "the identity write failed, so no identity is durable")
	require.Zero(t, r.PartCount(),
		"nothing may be committed once the identity write failed: the bucket index is the commit point")
}

// TestUncommittedPartIdentityIsNotLoaded verifies the other half of the ordering. The identity
// object lands but the bucket index does not, so the part was never committed: because identity is
// scoped to the part, the orphan carries its identities with it and recovery loads neither. (With a
// whole-set identity object at the engine prefix, the identity would instead have been stranded
// there — resolvable but backed by nothing, forever.)
func TestUncommittedPartIdentityIsNotLoaded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	cfg := engine.Config{Backend: be, Prefix: "t/metrics"}

	w := engine.New(cfg)
	mustAppend(t, w, mkSeries("job", "api"), 100, 1.0)

	// The part's objects land, the bucket index does not: an orphan part, swept at the next open.
	rejectWrites(be, "/"+bucketindex.Object, errWriteRejected)
	require.Error(t, w.Flush(ctx))
	be.Reset()

	r := engine.New(cfg)
	require.NoError(t, r.LoadParts(ctx))
	require.Zero(t, r.PartCount(), "the part was never committed")
	require.Zero(t, r.SeriesCount(), "its identities were never loaded — they live with the part")

	got := fetchAll(t, r, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	assert.Empty(t, got, "nothing resolves to the uncommitted part")
}

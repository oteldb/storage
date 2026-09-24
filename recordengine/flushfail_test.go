package recordengine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/recordengine"
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

// streamBodies returns every body the engine holds for the "api" stream, over all time.
func streamBodies(t *testing.T, e *recordengine.Engine) []string {
	t.Helper()

	batches := fetchAll(t, e, req("api"))

	out := make([]string, 0, len(batches))
	for _, b := range batches {
		out = append(out, bodies(b)...)
	}

	return out
}

// TestFlushFailureRestoresSideStore verifies the side-store snapshot taken with the head detach is
// restored when the flush fails, so the retried flush writes sidecars that still cover the records.
func TestFlushFailureRestoresSideStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	fs := newFakeSide()
	e := sideEngine(be, fs)

	b := mkBatch("api", rrec{ts: 100, body: "buffered"})
	b.Side = encodeSide(map[uint64][]byte{1: []byte("a"), 2: []byte("b")})
	ingest(t, e, b)

	rejectWrites(be, "", errWriteRejected)
	require.Error(t, e.Flush(ctx))
	be.Reset()

	require.Equal(t, 1, fs.restores)
	require.Len(t, fs.acc, 2, "the snapshot is back in the live accumulator")

	// A record appended during the failed flush contributes its own symbols; both sets must land.
	b2 := mkBatch("api", rrec{ts: 200, body: "later"})
	b2.Side = encodeSide(map[uint64][]byte{3: []byte("c")})
	ingest(t, e, b2)

	require.NoError(t, e.Flush(ctx))
	require.Equal(t, []uint64{1, 2, 3}, sideIDs(t, be))
}

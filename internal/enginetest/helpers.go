package enginetest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

func api(ts, val int64) Row { return Row{Stream: "api", Ts: ts, Val: val} }

// rejectReads fails every Read of a key ending in suffix with err. On "/manifest" it aborts a flush
// after the part's objects are fully written (at the openPart read-back), which is what leaves an
// orphan part behind.
func rejectReads(be *faultbackend.Backend, suffix string, err error) {
	be.Add(faultbackend.Rule{
		Kind:  faultbackend.Read,
		Match: func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, suffix) },
		Err:   err,
	})
}

func (k Kind) loadIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, k.indexKey())
	require.NoError(t, err)

	return ix
}

// indexCommit matches the compare-and-swaps against the bucket index.
func (k Kind) indexCommit(op faultbackend.Op) bool {
	return op.Kind == faultbackend.CompareAndSwap && op.Key == k.indexKey()
}

// partKeys returns the backend objects of the part with the given id.
func (k Kind) partKeys(ctx context.Context, t *testing.T, be backend.Backend, id string) []string {
	t.Helper()

	keys, err := be.List(ctx, k.Prefix+"/"+id+"/")
	require.NoError(t, err)

	return keys
}

// erasePart deletes every backend object of the part with the given id, the disk failure a repair
// exists for.
func (k Kind) erasePart(ctx context.Context, t *testing.T, be backend.Backend, id string) {
	t.Helper()

	for _, key := range k.partKeys(ctx, t, be, id) {
		require.NoError(t, be.Delete(ctx, key))
	}
}

func (k Kind) partDirs(ctx context.Context, t *testing.T, be backend.Backend) []string {
	t.Helper()

	return backendtest.PartDirs(ctx, t, be, k.Prefix)
}

// flushEach flushes one part per row and returns the part ids in flush order.
func (k Kind) flushEach(t *testing.T, e Engine, be backend.Backend, rows ...Row) []string {
	t.Helper()

	for _, r := range rows {
		e.Append(t, r)
		require.NoError(t, e.Flush(context.Background()))
	}

	ids := k.partDirs(context.Background(), t, be)
	require.Len(t, ids, len(rows))

	return ids
}

// twoParts flushes two parts and returns their ids in index order.
func (k Kind) twoParts(t *testing.T, e Engine, be backend.Backend) []string {
	t.Helper()

	return k.flushEach(t, e, be, api(100, 1), api(200, 2))
}

func prefixes(entries []bucketindex.Entry) []string {
	out := make([]string, len(entries))
	for i := range entries {
		out[i] = entries[i].Prefix
	}

	return out
}

func wantPrefixes(wants []bucketindex.Want) []string {
	out := make([]string, len(wants))
	for i := range wants {
		out[i] = wants[i].Prefix
	}

	return out
}

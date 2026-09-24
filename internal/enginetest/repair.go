package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func (k Kind) openRepair(t *testing.T, be backend.Backend, f PartFetcher) Engine {
	t.Helper()

	return k.Open(t, Config{Backend: be, Repair: f})
}

// CopyObjects copies every object under prefix from src to dst, which is what a repair fetch does
// to the objects of a part it pulls back. It fails t when prefix holds nothing.
func CopyObjects(t *testing.T, src, dst backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := src.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, key := range keys {
		data, err := backend.ReadView(ctx, src, key)
		require.NoError(t, err)
		require.NoError(t, dst.Write(ctx, key, data))
	}
}

// dropObjects deletes every object of a part, simulating the disk loss a want records.
func dropObjects(t *testing.T, be backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := be.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, key := range keys {
		require.NoError(t, be.Delete(ctx, key))
	}
}

// flushTwo writes api(100, 1) and api(200, 2) as one part each, returning the part prefixes.
func flushTwo(t *testing.T, e Engine) []string {
	t.Helper()
	ctx := context.Background()

	for _, r := range []Row{api(100, 1), api(200, 2)} {
		e.Append(t, r)
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, 2)

	return parts
}

// loseFirstOfTwo flushes two parts, destroys the first, records the want its absence owes and
// commits that loss with a third flush, returning the lost prefix.
func loseFirstOfTwo(t *testing.T, e Engine, be backend.Backend) string {
	t.Helper()

	lost := flushTwo(t, e)[0]

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	e.Append(t, api(300, 3))
	require.NoError(t, e.Flush(context.Background()))

	return lost
}

// mergeTimes runs n maintenance merges, which is how many repair passes the engine gets.
func mergeTimes(t *testing.T, e Engine, n int) {
	t.Helper()

	for range n {
		require.NoError(t, e.Merge(context.Background(), 0))
	}
}

// satisfyFrom answers a want by copying the exact part back from peer.
func satisfyFrom(t *testing.T, peer, be backend.Backend) Answer {
	t.Helper()

	return func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peer, be, w.Prefix)

		return bucketindex.Entry{
			Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks,
		}, bucketindex.WantSatisfied, nil
	}
}

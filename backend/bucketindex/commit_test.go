package bucketindex_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// TestSaveDoesNotDropAConcurrentWritersPart isolates the commit itself from the engines above it.
// Two writers load one index, each adds a part, and each saves. A save may lose the race — but it
// must then say so, so its caller can reload and retry. Reporting success while dropping the other
// writer's entry is the failure this asserts against, and is what a shared object store gets today
// (see #392): the entry names a part whose objects are in the store, durable and unreachable.
//
// The assertion is deliberately loose about the mechanism — a conditional write and a
// generation-named object both satisfy it.
func TestSaveDoesNotDropAConcurrentWritersPart(t *testing.T) {
	t.Skip("reproducer for #392: the bucket index is committed without compare-and-swap")
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	const key = "default/metrics/" + bucketindex.Object

	base := &bucketindex.Index{}
	base.Add(bucketindex.Entry{Prefix: "default/metrics/0000000000", MinTime: 1, MaxTime: 2})
	require.NoError(t, base.Save(ctx, be, key))

	a, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)
	b, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	a.Add(bucketindex.Entry{Prefix: "default/metrics/0000000001", MinTime: 3, MaxTime: 4})
	b.Add(bucketindex.Entry{Prefix: "default/metrics/0000000002", MinTime: 5, MaxTime: 6})

	require.NoError(t, a.Save(ctx, be, key))

	if err := b.Save(ctx, be, key); err != nil {
		return // The loser is told, which is all this asks for.
	}

	got, err := bucketindex.Load(ctx, be, key)
	require.NoError(t, err)

	var prefixes []string
	for _, e := range got.Entries {
		prefixes = append(prefixes, e.Prefix)
	}

	require.Contains(t, prefixes, "default/metrics/0000000001",
		"a save reporting success must not drop an entry committed since it was prepared")
	require.Contains(t, prefixes, "default/metrics/0000000002")
}

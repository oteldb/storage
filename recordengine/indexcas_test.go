package recordengine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// TestFlushRebasesOnARivalIndexCommit is the shared-store case of #392, with the interleaving
// stated rather than raced for: the flush is suspended *inside* its conditional index write, a
// rival writer commits a part of its own over the same key, and the flush is released. It must
// lose, reload, and commit an index naming both parts — the entry is the only thing keeping
// either part reachable.
func TestFlushRebasesOnARivalIndexCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		prefix = "t/recs"
		rival  = prefix + "/0000009999"
	)

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	other := &bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	other.Add(bucketindex.Entry{Prefix: rival, MinTime: 1, MaxTime: 2})
	_, err := other.Save(ctx, inner, prefix+"/"+bucketindex.Object, backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()

	require.NoError(t, <-flushed, "a flush that rebases still lands, and only reports success once it has")

	ix, err := bucketindex.Load(ctx, inner, prefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	prefixes := make([]string, 0, len(ix.Entries))
	for _, ent := range ix.Entries {
		prefixes = append(prefixes, ent.Prefix)
	}

	require.Contains(t, prefixes, rival, "the rival's part must survive this engine's commit")
	require.Len(t, prefixes, 2, "and this engine's own flushed part is committed alongside it")
}

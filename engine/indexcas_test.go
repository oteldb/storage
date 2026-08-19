package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/engine"
)

// TestFlushRebasesOnARivalIndexCommit is the shared-store case of #392 with the interleaving
// stated rather than raced for: the engine's flush is suspended *inside* its conditional index
// write, a rival writer commits a part of its own over the same key, and the flush is released.
// The flush must not overwrite the rival's entry — it loses, reloads, and commits an index naming
// both parts, because the entry is the only thing keeping either part reachable.
func TestFlushRebasesOnARivalIndexCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		prefix = "default/metrics"
		rival  = prefix + "/0000009999"
	)

	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, func(op faultbackend.Op) bool {
		return strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	e := engine.New(engine.Config{Backend: be, Prefix: prefix})
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	flushed := make(chan error, 1)
	go func() { flushed <- e.Flush(ctx) }()

	gate.Await(t)

	// The rival commits through the raw backend, so it is not itself gated.
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

// TestFlushFailsWhenTheIndexCommitCannotLand covers the other end of the retry loop: a commit that
// never wins must fail the flush. A part whose entry is not in the index is unreachable, and the
// next open sweeps it — reporting success over that is the data loss.
func TestFlushFailsWhenTheIndexCommitCannotLand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const prefix = "default/metrics"

	be := &alwaysLosesIndexCommit{Backend: backend.Memory()}
	e := engine.New(engine.Config{Backend: be, Prefix: prefix})
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	require.ErrorIs(t, e.Flush(ctx), bucketindex.ErrConflict)
}

// alwaysLosesIndexCommit refuses every conditional index write, as an endlessly contended prefix
// would. The retry loop must give up and say so rather than spin.
type alwaysLosesIndexCommit struct {
	backend.Backend
}

func (b *alwaysLosesIndexCommit) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	if strings.HasSuffix(key, "/"+bucketindex.Object) {
		return backend.VersionAbsent, false, nil
	}

	return b.Backend.CompareAndSwap(ctx, key, expected, data)
}

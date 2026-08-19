package bucketindex_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
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
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	const key = "default/metrics/" + bucketindex.Object

	base := &bucketindex.Index{}
	base.Add(bucketindex.Entry{Prefix: "default/metrics/0000000000", MinTime: 1, MaxTime: 2})
	_, err := base.Save(ctx, be, key, backend.VersionAbsent)
	require.NoError(t, err)

	a, aVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	b, bVersion, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	a.Add(bucketindex.Entry{Prefix: "default/metrics/0000000001", MinTime: 3, MaxTime: 4})
	b.Add(bucketindex.Entry{Prefix: "default/metrics/0000000002", MinTime: 5, MaxTime: 6})

	_, err = a.Save(ctx, be, key, aVersion)
	require.NoError(t, err)

	if _, err := b.Save(ctx, be, key, bVersion); err != nil {
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

// TestSaveLosesGatedRaceAndRetryLands states the interleaving instead of racing for it: writer A
// is suspended *inside* its conditional write, writer B commits over the version both of them
// read, and A is released. A must be told it lost, and its reload-and-retry must then land both
// parts — the loop every committer above this package runs.
func TestSaveLosesGatedRaceAndRetryLands(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const key = "default/metrics/" + bucketindex.Object

	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.CompareAndSwap, nil))

	a, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)
	require.Equal(t, backend.VersionAbsent, version)

	a.Add(bucketindex.Entry{Prefix: "default/metrics/0000000001", MinTime: 3, MaxTime: 4})

	type outcome struct {
		version backend.Version
		err     error
	}

	done := make(chan outcome, 1)

	go func() {
		v, err := a.Save(ctx, be, key, version)
		done <- outcome{version: v, err: err}
	}()

	gate.Await(t)

	// B commits while A is held inside its conditional write. It goes through the raw backend so
	// it is not itself gated.
	b := &bucketindex.Index{}
	b.Add(bucketindex.Entry{Prefix: "default/metrics/0000000002", MinTime: 5, MaxTime: 6})
	_, err = b.Save(ctx, be.Backend, key, backend.VersionAbsent)
	require.NoError(t, err)

	gate.Release()

	res := <-done
	require.ErrorIs(t, res.err, bucketindex.ErrConflict, "the suspended writer must be told it lost")

	// The retry: reload, rebuild on what is there, commit again.
	got, version, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	got.Add(bucketindex.Entry{Prefix: "default/metrics/0000000001", MinTime: 3, MaxTime: 4})
	_, err = got.Save(ctx, be, key, version)
	require.NoError(t, err)

	final, _, err := bucketindex.LoadVersioned(ctx, be, key)
	require.NoError(t, err)

	prefixes := make([]string, 0, len(final.Entries))
	for _, e := range final.Entries {
		prefixes = append(prefixes, e.Prefix)
	}

	require.ElementsMatch(t,
		[]string{"default/metrics/0000000001", "default/metrics/0000000002"}, prefixes)
}

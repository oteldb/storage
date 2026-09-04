package partsync_test

// An owner merge landing between the two reads a pass makes of the peer — the bucket index first,
// the key listing second — leaves the pass holding an index that names a part the listing no
// longer offers. Installing it publishes an entry that resolves to objects the replica does not
// have. The interleaving is stated with a gate on the owner's List, not raced for.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/cluster/partsync"
)

// requireIndexResolves reads every object of every part the local index names, which is what a
// reader loading this backend does. A missing object is the failure the commit-point ordering
// exists to prevent.
func requireIndexResolves(t *testing.T, local backend.Backend, prefix string) *bucketindex.Index {
	t.Helper()
	ctx := context.Background()

	raw, err := backend.ReadView(ctx, local, prefix+"/"+bucketindex.Object)
	require.NoError(t, err, "the local index is readable")

	ix, err := bucketindex.Decode(raw)
	require.NoError(t, err)

	for _, e := range ix.Entries {
		for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
			_, err := backend.ReadView(ctx, local, e.Prefix+suffix)
			require.NoErrorf(t, err, "index entry %q resolves to %q", e.Prefix, e.Prefix+suffix)
		}
	}

	return ix
}

// partPrefixes is the parts an index names, for readable assertions.
func partPrefixes(ix *bucketindex.Index) []string {
	out := make([]string, 0, len(ix.Entries))
	for _, e := range ix.Entries {
		out = append(out, e.Prefix)
	}

	return out
}

func TestSyncNeverInstallsIndexMergedAwayUnderIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/metrics"

	owner := backend.Memory()
	gated := faultbackend.Wrap(owner)
	addr := serve(t, gated)

	ix := &bucketindex.Index{}
	writePart(t, owner, ix, prefix, 1, 100, 200)
	saveIndex(t, owner, prefix, ix)
	require.NoError(t, owner.Write(ctx, prefix+"/streams.bin", []byte("streams-v1")))

	replica := backend.Memory()
	s := partsync.New(replica, &partsync.Client{})

	// A quiet first pass gives the replica a good index to keep if the racing one is rejected.
	st, err := s.Sync(ctx, prefix, []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Equal(t, []string{prefix + "/0000000001"}, partPrefixes(requireIndexResolves(t, replica, prefix)))

	// The owner flushes part 2, so the next pass has something newer to mirror.
	writePart(t, owner, ix, prefix, 2, 300, 400)
	saveIndex(t, owner, prefix, ix)

	// Suspend the pass at the owner's key listing — after it has read the index naming parts 1
	// and 2, before it learns which objects the owner still has.
	gate := faultbackend.NewGate()
	gated.Add(gate.Rule(faultbackend.List, nil))

	done := make(chan partsync.Stats, 1)
	errc := make(chan error, 1)

	go func() {
		st, err := s.Sync(ctx, prefix, []string{addr}, false, nil)
		errc <- err
		done <- st
	}()

	gate.Await(t)

	// The merge the pass is racing: parts 1 and 2 become part 3, and the owner's index says so.
	for _, suffix := range []string{"/c/0", "/marks", "/manifest"} {
		require.NoError(t, owner.Delete(ctx, prefix+"/0000000002"+suffix))
	}

	merged := &bucketindex.Index{}
	writePart(t, owner, merged, prefix, 1, 100, 200)
	writePart(t, owner, merged, prefix, 3, 100, 400)
	saveIndex(t, owner, prefix, merged)

	gate.Release()

	require.NoError(t, <-errc)
	<-done

	// The pass held an index naming part 2, whose objects the owner had already dropped: nothing
	// it copied can back that entry, so it must not become the replica's index.
	got := requireIndexResolves(t, replica, prefix)
	assert.NotContains(t, partPrefixes(got), prefix+"/0000000002",
		"a part the pass could not copy is not published")

	// And the next, unraced pass converges on the merged state.
	_, err = s.Sync(ctx, prefix, []string{addr}, false, nil)
	require.NoError(t, err)

	assert.Equal(t,
		[]string{prefix + "/0000000001", prefix + "/0000000003"},
		partPrefixes(requireIndexResolves(t, replica, prefix)))
}

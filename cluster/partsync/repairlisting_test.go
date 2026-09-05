package partsync_test

// A peer's index is not evidence of what its disk holds, so a want no index names is asked of the
// peers' listings. The listing is read once per peer for the batch — the same per-cycle cost the
// index reads have — and only a complete copy (the manifest is there) counts.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// stageHeldButUnindexed writes part 0001 to be and saves an index that does not name it as data:
// carrying a want for it, or a hole, or nothing at all.
func stageHeldButUnindexed(t *testing.T, be backend.Backend, prefix string, claim func(*bucketindex.Index, bucketindex.Entry)) bucketindex.Entry {
	t.Helper()

	staging := &bucketindex.Index{}
	writeBlockPart(t, be, staging, prefix, "0001", bucketindex.Interval{Min: 1, Max: 1}, 0)
	ent := staging.Entries[0]

	ix := &bucketindex.Index{Generation: gen(1, 2)}
	if claim != nil {
		claim(ix, ent)
	}

	saveIndex(t, be, prefix, ix)

	return ent
}

func TestFetchWantFallsBackToPeerDisk(t *testing.T) {
	t.Parallel()

	const prefix = "default/logs"

	claims := map[string]func(*bucketindex.Index, bucketindex.Entry){
		"wanted": func(ix *bucketindex.Index, e bucketindex.Entry) { ix.RecordWant(bucketindex.WantOf(e, ix.Generation)) },
		"holed":  func(ix *bucketindex.Index, e bucketindex.Entry) { ix.RecordHole(bucketindex.WantOf(e, ix.Generation)) },
		"silent": nil,
	}

	for name, claim := range claims {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			holder, damaged := backend.Memory(), backend.Memory()
			ent := stageHeldButUnindexed(t, holder, prefix, claim)
			peer := servePeer(t, holder, prefix, probeOpts{})

			s := partsync.New(damaged, &partsync.Client{})

			got, ok, err := s.FetchWant(ctx, prefix, []string{peer.addr}, bucketindex.WantOf(ent, gen(0, 0)))
			require.NoError(t, err)
			require.True(t, ok, "the disk holds the part whatever the index says")
			assert.Equal(t, ent, got, "the want's own identity comes back")

			keys, err := damaged.List(ctx, ent.Prefix)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{ent.Prefix + "/c/0", ent.Prefix + "/manifest"}, keys)

			assert.Equal(t, 1, peer.indexFetches(prefix))
			assert.Equal(t, 2, peer.listCalls(), "one listing of the prefix, one of the part being copied")
		})
	}
}

func TestFetchWantRemnantsWithoutManifestAreNotAPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	holder, damaged := backend.Memory(), backend.Memory()
	ent := stageHeldButUnindexed(t, holder, prefix, nil)
	require.NoError(t, holder.Delete(ctx, ent.Prefix+"/manifest"))

	peer := servePeer(t, holder, prefix, probeOpts{})
	s := partsync.New(damaged, &partsync.Client{})

	_, ok, err := s.FetchWant(ctx, prefix, []string{peer.addr}, bucketindex.WantOf(ent, gen(0, 0)))
	require.NoError(t, err, "every peer answered: the absence is definitive")
	assert.False(t, ok, "surviving objects are not a readable part")
}

func TestFetchWantListingFailureIsTransient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	holder, damaged := backend.Memory(), backend.Memory()
	ent := stageHeldButUnindexed(t, holder, prefix, nil)

	peer := servePeer(t, holder, prefix, probeOpts{failList: true})
	s := partsync.New(damaged, &partsync.Client{})

	_, ok, err := s.FetchWant(ctx, prefix, []string{peer.addr}, bucketindex.WantOf(ent, gen(0, 0)))
	require.Error(t, err, "a disk that could not be listed is not evidence of absence")
	assert.False(t, ok)
}

// TestFetchWantsListsEachPeerOncePerCycle pins the fallback's cost: wants no index names share one
// listing of each peer, so the count is O(peers) like the index reads, not O(peers × wants).
func TestFetchWantsListsEachPeerOncePerCycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	peers := make([]*probePeer, 3)
	addrs := make([]string, len(peers))

	names := []string{"0001", "0002", "0003", "0004"}
	wants := make([]bucketindex.Want, len(names))

	for i := range peers {
		be := backend.Memory()

		staging := &bucketindex.Index{}
		for j, name := range names {
			writeBlockPart(t, be, staging, prefix, name, bucketindex.Interval{Min: uint64(j + 1), Max: uint64(j + 1)}, 0)
			wants[j] = bucketindex.WantOf(staging.Entries[j], gen(0, 0))
		}

		saveIndex(t, be, prefix, &bucketindex.Index{Generation: gen(1, 1)})

		peers[i] = servePeer(t, be, prefix, probeOpts{})
		addrs[i] = peers[i].addr
	}

	s := partsync.New(backend.Memory(), &partsync.Client{}, partsync.WithRandSeed(11))

	for _, r := range s.FetchWants(ctx, prefix, addrs, wants) {
		require.NoError(t, r.Err)
		require.True(t, r.OK)
	}

	indexReads, lists := 0, 0
	for _, p := range peers {
		assert.LessOrEqual(t, p.indexFetches(prefix), 1)

		indexReads += p.indexFetches(prefix)
		lists += p.listCalls()
	}

	assert.Equal(t, len(peers), indexReads, "one index read per peer")
	assert.Equal(t, len(peers)+len(wants), lists,
		"one prefix listing per peer for the batch, plus one part listing per copy")
}

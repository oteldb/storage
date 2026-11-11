package partsync_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

func seriesRootObjects(t *testing.T, be backend.Backend, prefix string, ix *bucketindex.Index) {
	t.Helper()
	root := prefix + "/s-root"
	ix.SeriesRoots = []bucketindex.SeriesRoot{{Prefix: root, ManifestDigest: [32]byte{1}}}
	require.NoError(t, be.Write(t.Context(), root+"/manifest", []byte("metadata manifest")))
	require.NoError(t, be.Write(t.Context(), root+"/series-index", []byte("metadata index")))
}

func snapshotObjects(t *testing.T, be backend.Backend) map[string][]byte {
	t.Helper()
	keys, err := be.List(t.Context(), "")
	require.NoError(t, err)
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		out[key], err = be.Read(t.Context(), key)
		require.NoError(t, err)
	}
	return out
}

func TestSyncRejectsSeparateSeriesWithoutMutation(t *testing.T) {
	t.Parallel()
	for _, location := range []string{"local", "peer", "older peer"} {
		t.Run(location, func(t *testing.T) {
			t.Parallel()
			local, peer, old := backend.Memory(), backend.Memory(), backend.Memory()
			const prefix = "tenant/metrics"
			localIndex := &bucketindex.Index{Generation: gen(1, 1)}
			peerIndex := &bucketindex.Index{Generation: gen(1, 2)}
			oldIndex := &bucketindex.Index{Generation: gen(1, 0)}
			writePart(t, local, localIndex, prefix, 1, 1, 2)
			writePart(t, peer, peerIndex, prefix, 2, 3, 4)
			switch location {
			case "local":
				seriesRootObjects(t, local, prefix, localIndex)
			case "peer":
				seriesRootObjects(t, peer, prefix, peerIndex)
			case "older peer":
				seriesRootObjects(t, old, prefix, oldIndex)
			}
			saveIndex(t, local, prefix, localIndex)
			saveIndex(t, peer, prefix, peerIndex)
			saveIndex(t, old, prefix, oldIndex)
			beforeLocal, beforePeer, beforeOld := snapshotObjects(t, local), snapshotObjects(t, peer), snapshotObjects(t, old)
			s := partsync.New(local, &partsync.Client{})
			peers := []string{serve(t, peer), serve(t, old)}
			for range 3 {
				stats, err := s.Sync(t.Context(), prefix, peers, false, func(string) bool { return true })
				require.ErrorContains(t, err, "separate series catalogs are unsupported")
				require.Zero(t, stats.Copied)
				require.Zero(t, stats.Pruned)
				require.Equal(t, beforeLocal, snapshotObjects(t, local))
				require.Equal(t, beforePeer, snapshotObjects(t, peer))
				require.Equal(t, beforeOld, snapshotObjects(t, old))
			}
		})
	}
}

func TestRepairRejectsSeparateSeriesWithoutFallback(t *testing.T) {
	t.Parallel()
	for _, location := range []string{"local", "peer"} {
		t.Run(location, func(t *testing.T) {
			t.Parallel()
			local, peer, invalid := backend.Memory(), backend.Memory(), backend.Memory()
			const prefix = "tenant/metrics"
			localIndex, peerIndex := &bucketindex.Index{}, &bucketindex.Index{}
			part := writeBlockPart(t, peer, peerIndex, prefix, "numeric", bucketindex.Range(0, 1, 1), 0)
			if location == "local" {
				seriesRootObjects(t, local, prefix, localIndex)
			} else {
				seriesRootObjects(t, peer, prefix, peerIndex)
			}
			saveIndex(t, local, prefix, localIndex)
			saveIndex(t, peer, prefix, peerIndex)
			require.NoError(t, invalid.Write(t.Context(), prefix+"/"+bucketindex.Object, []byte("corrupt")))
			beforeLocal, beforePeer := snapshotObjects(t, local), snapshotObjects(t, peer)
			s := partsync.New(local, &partsync.Client{})
			results := s.FetchWants(t.Context(), prefix, []string{serve(t, invalid), serve(t, peer)},
				[]bucketindex.Want{{Prefix: part, Blocks: bucketindex.Range(0, 1, 1)}})
			require.Len(t, results, 1)
			require.ErrorContains(t, results[0].Err, "separate series catalogs are unsupported")
			require.False(t, results[0].OK)
			require.Equal(t, beforeLocal, snapshotObjects(t, local))
			require.Equal(t, beforePeer, snapshotObjects(t, peer))
		})
	}
}

func TestBrokenSeriesCatalogCannotAuthorizeSyncOrRepair(t *testing.T) {
	t.Parallel()
	for _, location := range []string{"local", "peer"} {
		for _, damage := range []string{"truncated", "future version"} {
			t.Run(location+"/"+damage, func(t *testing.T) {
				t.Parallel()
				local, peer, legacy := backend.Memory(), backend.Memory(), backend.Memory()
				const prefix = "tenant/metrics"
				key := prefix + "/" + bucketindex.Object
				localIndex := &bucketindex.Index{Generation: gen(1, 1)}
				peerIndex := &bucketindex.Index{Generation: gen(1, 2)}
				writePart(t, local, localIndex, prefix, 1, 1, 2)
				writePart(t, peer, peerIndex, prefix, 2, 3, 4)
				broken := peer
				if location == "local" {
					broken = local
					seriesRootObjects(t, local, prefix, localIndex)
				} else {
					seriesRootObjects(t, peer, prefix, peerIndex)
				}
				saveIndex(t, local, prefix, localIndex)
				saveIndex(t, peer, prefix, peerIndex)
				saveIndex(t, legacy, prefix, &bucketindex.Index{Generation: gen(1, 3)})
				raw, err := broken.Read(t.Context(), key)
				require.NoError(t, err)
				if damage == "truncated" {
					raw = raw[:len(raw)-1]
				} else {
					raw[2] = 10
				}
				require.NoError(t, broken.Write(t.Context(), key, raw))
				beforeLocal, beforePeer := snapshotObjects(t, local), snapshotObjects(t, peer)
				s := partsync.New(local, &partsync.Client{})
				peers := []string{serve(t, legacy), serve(t, peer)}
				stats, err := s.Sync(t.Context(), prefix, peers, false, func(string) bool { return true })
				require.ErrorContains(t, err, "separate series catalogs are unsupported")
				require.Zero(t, stats.Copied)
				require.Zero(t, stats.Pruned)
				results := s.FetchWants(t.Context(), prefix, peers,
					[]bucketindex.Want{{Prefix: peerIndex.Entries[0].Prefix}})
				require.Len(t, results, 1)
				require.ErrorContains(t, results[0].Err, "separate series catalogs are unsupported")
				require.False(t, results[0].OK)
				require.Equal(t, beforeLocal, snapshotObjects(t, local))
				require.Equal(t, beforePeer, snapshotObjects(t, peer))
			})
		}
	}
}

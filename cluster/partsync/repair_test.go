package partsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// writeBlockPart is [writePart] for a part carrying block identity, which is what makes a want
// satisfiable by something other than its exact prefix.
func writeBlockPart(
	t *testing.T, be backend.Backend, ix *bucketindex.Index, prefix, name string,
	blocks bucketindex.Interval, level uint32,
) string {
	t.Helper()
	ctx := context.Background()

	p := prefix + "/" + name
	require.NoError(t, be.Write(ctx, p+"/c/0", []byte("col0-"+p)))
	require.NoError(t, be.Write(ctx, p+"/manifest", []byte("manifest-"+p)))
	ix.Add(bucketindex.Entry{Prefix: p, MinTime: 1, MaxTime: 2, Blocks: blocks, Level: level})

	return p
}

func TestFetchWantExactPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, damaged := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{}
	part := writeBlockPart(t, owner, ix, "default/logs", "0001", bucketindex.Interval{Min: 1, Max: 1}, 0)
	saveIndex(t, owner, "default/logs", ix)

	s := partsync.New(damaged, &partsync.Client{})

	ent, ok, err := s.FetchWant(ctx, "default/logs", []string{serve(t, owner)},
		bucketindex.Want{Prefix: part, Blocks: bucketindex.Interval{Min: 1, Max: 1}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, part, ent.Prefix)

	keys, err := damaged.List(ctx, part)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{part + "/c/0", part + "/manifest"}, keys)

	// The peer's index is deliberately not installed: committing the entry is the owner's job.
	_, err = backend.ReadView(ctx, damaged, "default/logs/"+bucketindex.Object)
	require.ErrorIs(t, err, backend.ErrNotExist)
}

func TestFetchWantPrefersContainingSuccessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, damaged := backend.Memory(), backend.Memory()

	// The wanted part is gone from the peer too — merged into a level-1 successor covering it.
	ix := &bucketindex.Index{}
	merged := writeBlockPart(t, owner, ix, "default/logs", "0100", bucketindex.Interval{Min: 1, Max: 4}, 1)
	saveIndex(t, owner, "default/logs", ix)

	s := partsync.New(damaged, &partsync.Client{})

	ent, ok, err := s.FetchWant(ctx, "default/logs", []string{serve(t, owner)},
		bucketindex.Want{Prefix: "default/logs/0002", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, merged, ent.Prefix, "the successor containing the want, not the vanished prefix")

	keys, err := damaged.List(ctx, merged)
	require.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestFetchWantPicksWidestAcrossPeers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	narrow, wide, damaged := backend.Memory(), backend.Memory(), backend.Memory()

	nix := &bucketindex.Index{}
	writeBlockPart(t, narrow, nix, "default/logs", "0002", bucketindex.Interval{Min: 2, Max: 2}, 0)
	saveIndex(t, narrow, "default/logs", nix)

	wix := &bucketindex.Index{}
	big := writeBlockPart(t, wide, wix, "default/logs", "0200", bucketindex.Interval{Min: 1, Max: 8}, 2)
	saveIndex(t, wide, "default/logs", wix)

	s := partsync.New(damaged, &partsync.Client{})

	ent, ok, err := s.FetchWant(ctx, "default/logs", []string{serve(t, narrow), serve(t, wide)},
		bucketindex.Want{Prefix: "default/logs/0002", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, big, ent.Prefix)
}

func TestFetchWantNoPeerHasIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, damaged := backend.Memory(), backend.Memory()

	ix := &bucketindex.Index{}
	writeBlockPart(t, owner, ix, "default/logs", "0009", bucketindex.Interval{Min: 9, Max: 9}, 0)
	saveIndex(t, owner, "default/logs", ix)

	s := partsync.New(damaged, &partsync.Client{})

	_, ok, err := s.FetchWant(ctx, "default/logs", []string{serve(t, owner)},
		bucketindex.Want{Prefix: "default/logs/0002", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	require.NoError(t, err, "every peer answered, so absence is definitive rather than an error")
	assert.False(t, ok)
}

func TestFetchWantUnreachablePeerIsTransient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := partsync.New(backend.Memory(), &partsync.Client{})

	_, ok, err := s.FetchWant(ctx, "default/logs", []string{"127.0.0.1:1"},
		bucketindex.Want{Prefix: "default/logs/0002", Blocks: bucketindex.Interval{Min: 2, Max: 2}})
	require.Error(t, err, "a peer we could not ask is not evidence the part is gone")
	assert.False(t, ok)
}

func TestFetchWantRejectsBadPrefix(t *testing.T) {
	t.Parallel()

	s := partsync.New(backend.Memory(), &partsync.Client{})

	_, ok, err := s.FetchWant(context.Background(), "../escape", nil, bucketindex.Want{Prefix: "x"})
	require.Error(t, err)
	assert.False(t, ok)
}

func TestFetchWantPartialPeerObjectsFail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner, damaged := backend.Memory(), backend.Memory()

	// An index entry the peer's objects do not back: the copy must not report a repaired part.
	ix := &bucketindex.Index{}
	ix.Add(bucketindex.Entry{
		Prefix: "default/logs/0001", MinTime: 1, MaxTime: 2,
		Blocks: bucketindex.Interval{Min: 1, Max: 1},
	})
	saveIndex(t, owner, "default/logs", ix)

	s := partsync.New(damaged, &partsync.Client{})

	_, ok, err := s.FetchWant(ctx, "default/logs", []string{serve(t, owner)},
		bucketindex.Want{Prefix: "default/logs/0001", Blocks: bucketindex.Interval{Min: 1, Max: 1}})
	require.Error(t, err)
	assert.False(t, ok)
}

package partsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/cluster/partsync"
)

// syncedInstall fails a test whose installed index names a part with deferred objects no
// SyncPrefix has covered.
type syncedInstall struct {
	*backendtest.Deferred

	t       *testing.T
	checked int
}

func (s *syncedInstall) Write(ctx context.Context, key string, data []byte) error {
	if key == "default/metrics/"+bucketindex.Object {
		ix, err := bucketindex.Decode(data)
		require.NoError(s.t, err)

		for i := range ix.Entries {
			assert.Empty(s.t, s.Pending(ix.Entries[i].Prefix), "installed index names part %q before it is synced", ix.Entries[i].Prefix)
			s.checked++
		}
	}

	return s.Deferred.Write(ctx, key, data)
}

func TestSyncSyncsPartsBeforeInstallingIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner := backend.Memory()
	ix := &bucketindex.Index{}
	writePart(t, owner, ix, "default/metrics", 1, 100, 200)
	writePart(t, owner, ix, "default/metrics", 2, 300, 400)
	saveIndex(t, owner, "default/metrics", ix)

	local := &syncedInstall{Deferred: backendtest.WithDeferred(backend.Memory()), t: t}

	st, err := partsync.New(local, &partsync.Client{}).Sync(ctx, "default/metrics", []string{serve(t, owner)}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Equal(t, 2, local.checked)
}

func TestFetchWantsSyncsTheCopiedPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const prefix = "default/logs"

	be := backend.Memory()
	ix := &bucketindex.Index{}
	merged := writeBlockPart(t, be, ix, prefix, "0100", bucketindex.Interval{Min: 1, Max: 4}, 1)
	saveIndex(t, be, prefix, ix)

	peer := servePeer(t, be, prefix, probeOpts{})
	local := backendtest.WithDeferred(backend.Memory())

	res := partsync.New(local, &partsync.Client{}, partsync.WithRandSeed(1)).
		FetchWants(ctx, prefix, []string{peer.addr}, []bucketindex.Want{wantAt(prefix+"/0001", 1)})

	require.Len(t, res, 1)
	require.NoError(t, res[0].Err)
	require.True(t, res[0].OK)

	keys, err := local.List(ctx, merged+"/")
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	assert.Empty(t, local.Pending(merged))
}

// failSync fails SyncPrefix until healed, modeling a sync that errored or a process that died
// before it: the part objects stay on disk, deferred.
type failSync struct {
	*syncedInstall

	failing bool
}

func (f *failSync) SyncPrefix(ctx context.Context, prefix string) error {
	if f.failing {
		return assert.AnError
	}

	return f.syncedInstall.SyncPrefix(ctx, prefix)
}

func TestSyncRetrySyncsPartsAnEarlierPassLeftDeferred(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	owner := backend.Memory()
	ix := &bucketindex.Index{}
	writePart(t, owner, ix, "default/metrics", 1, 100, 200)
	saveIndex(t, owner, "default/metrics", ix)
	addr := serve(t, owner)

	local := &failSync{syncedInstall: &syncedInstall{Deferred: backendtest.WithDeferred(backend.Memory()), t: t}, failing: true}
	s := partsync.New(local, &partsync.Client{})

	_, err := s.Sync(ctx, "default/metrics", []string{addr}, false, nil)
	require.ErrorIs(t, err, assert.AnError)
	assert.Zero(t, local.checked, "no index installed over a failed sync")

	local.failing = false

	st, err := s.Sync(ctx, "default/metrics", []string{addr}, false, nil)
	require.NoError(t, err)
	require.True(t, st.Synced)
	assert.Equal(t, 1, local.checked)
}

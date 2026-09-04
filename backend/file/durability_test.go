package file

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// Durability here means power loss, not a process crash: the fake keeps only what a directory sync
// committed, which is the one thing a real filesystem will not let a test observe on demand.

func read(t *testing.T, b *File, key string) []byte {
	t.Helper()

	data, err := b.Read(context.Background(), key)
	require.NoError(t, err)

	return data
}

func TestWriteSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()
	b := newFS(fsys)

	require.NoError(t, b.Write(context.Background(), "t1/metrics/0001/manifest", []byte("m")))

	after := newFS(fsys.Crash())
	assert.Equal(t, []byte("m"), read(t, after, "t1/metrics/0001/manifest"))
}

func TestPutIfAbsentSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()
	b := newFS(fsys)

	written, err := b.PutIfAbsent(context.Background(), "t1/lease", []byte("owner"))
	require.NoError(t, err)
	require.True(t, written)

	after := newFS(fsys.Crash())
	assert.Equal(t, []byte("owner"), read(t, after, "t1/lease"))

	// The claim must still be exclusive after the crash, or a second holder takes the lease.
	written, err = after.PutIfAbsent(context.Background(), "t1/lease", []byte("other"))
	require.NoError(t, err)
	assert.False(t, written)
}

// TestCompareAndSwapSurvivesPowerLoss covers the commit point: the bucket index is published by a
// CAS, and the WAL checkpoint that follows it deletes the segments replay would otherwise need. An
// index reverting under a power cut while the segments are gone is silent loss of acknowledged
// records, so this is the case the whole seam exists for.
func TestCompareAndSwapSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	v1, ok, err := b.CompareAndSwap(ctx, "t1/index", backend.VersionAbsent, []byte("epoch-1"))
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = b.CompareAndSwap(ctx, "t1/index", v1, []byte("epoch-2"))
	require.NoError(t, err)
	require.True(t, ok)

	after := newFS(fsys.Crash())
	got, version, err := after.ReadVersioned(ctx, "t1/index")
	require.NoError(t, err)
	assert.Equal(t, []byte("epoch-2"), got)
	assert.Equal(t, backend.ContentVersion([]byte("epoch-2")), version)
}

func TestCreateObjectSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	w, err := b.CreateObject(ctx, "t1/metrics/0001/c/col")
	require.NoError(t, err)
	_, err = w.Write([]byte("column"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))

	after := newFS(fsys.Crash())
	assert.Equal(t, []byte("column"), read(t, after, "t1/metrics/0001/c/col"))
}

func TestDeleteSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	require.NoError(t, b.Write(ctx, "t1/metrics/0001/manifest", []byte("m")))
	require.NoError(t, b.Delete(ctx, "t1/metrics/0001/manifest"))

	after := newFS(fsys.Crash())
	_, err := after.Read(ctx, "t1/metrics/0001/manifest")
	assert.ErrorIs(t, err, backend.ErrNotExist)
}

// TestWriteIsDurableOnlyAfterDirectorySync places the crash between the rename and the directory
// sync that follows it, so what is asserted is the *ordering*: up to that sync the object's bytes
// are on the disk with nothing naming them, and only the sync publishes the name.
func TestWriteIsDurableOnlyAfterDirectorySync(t *testing.T) {
	t.Parallel()

	const key = "t1/metrics/0001/manifest"

	gate := faultfs.NewGate()
	fsys := faultfs.New()
	fsys.Add(gate.Rule(faultfs.OpSyncDir, func(c faultfs.Call) bool { return c.Name == "t1/metrics/0001" }))

	done := make(chan error, 1)
	go func() { done <- newFS(fsys).Write(context.Background(), key, []byte("m")) }()

	held := gate.Await(t)
	require.Equal(t, "t1/metrics/0001", held.Name)

	// The rename has happened and its directory has not been synced: a power cut here takes the name.
	_, durable := fsys.Durable(key)
	assert.False(t, durable, "the name is durable before its directory was synced")

	// The same instant, with the process killed instead of the machine: nothing is lost.
	killed := newFS(fsys.Kill())
	assert.Equal(t, []byte("m"), read(t, killed, key))

	gate.Release()
	require.NoError(t, <-done)

	after := newFS(fsys.Crash())
	assert.Equal(t, []byte("m"), read(t, after, key))
}

// TestWriteSyncsCreatedDirectoryChain asserts the order the chain is committed in: a directory's
// own entry lives in its parent, so syncing only the leaf leaves the object named by a path whose
// upper components a power cut can still take away.
func TestWriteSyncsCreatedDirectoryChain(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()
	require.NoError(t, newFS(fsys).Write(context.Background(), "t1/metrics/0001/manifest", []byte("m")))

	var synced []string

	for _, c := range fsys.Calls() {
		if c.Op == faultfs.OpSyncDir {
			synced = append(synced, c.Name)
		}
	}

	assert.Equal(t, []string{".", "t1", "t1/metrics", "t1/metrics/0001"}, synced)
}

// TestWriteSyncsOneDirectoryWhenNoneAreCreated keeps the cost of the fix visible: publishing into a
// directory that already exists is one directory fsync, not one per path component.
func TestWriteSyncsOneDirectoryWhenNoneAreCreated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	require.NoError(t, b.Write(ctx, "t1/metrics/0001/a", []byte("a")))

	before := len(fsys.Calls())
	require.NoError(t, b.Write(ctx, "t1/metrics/0001/b", []byte("b")))

	var synced int

	for _, c := range fsys.Calls()[before:] {
		if c.Op == faultfs.OpSyncDir {
			synced++
		}
	}

	assert.Equal(t, 1, synced)
}

// TestWriteReportsDirectorySyncFailure keeps the fsync on the error path: a directory sync that
// fails is a write that is not durable, and reporting success there is the bug this file is about
// in its loudest form.
func TestWriteReportsDirectorySyncFailure(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()
	fsys.Add(faultfs.Rule{Op: faultfs.OpSyncDir, Err: assert.AnError})

	err := newFS(fsys).Write(context.Background(), "t1/obj", []byte("m"))
	assert.ErrorIs(t, err, assert.AnError)
}

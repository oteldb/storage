package file

import (
	"context"
	"slices"
	"strconv"
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

// TestWriteSyncsCreatedDirectoryChain asserts the chain is committed innermost first: a directory's
// own entry lives in its parent, so syncing only the leaf leaves the object named by a path whose
// upper components a power cut can still take away, and syncing an entry before what it names lets
// the disk hold a directory whose contents are not there yet.
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

	assert.Equal(t, []string{"t1/metrics/0001", "t1/metrics", "t1", "."}, synced)
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

// A part directory can be removed by compaction or retention while a listing walks the tenant
// above it, which is how a partsync peer listing met "openat …: no such file or directory".
func TestListSkipsDirectoryRemovedMidWalk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	const doomed = "t1/traces/01M22YPD9JSHCNTBC1ABBM90G0"
	require.NoError(t, b.Write(ctx, doomed+"/manifest", []byte("m")))
	require.NoError(t, b.Write(ctx, "t1/traces/01M22YPD9JSHCNTBC1ABBM90G1/manifest", []byte("m")))

	// Delete the part the moment the walk descends into it, so the ReadDir that follows finds
	// nothing — the race, without a second goroutine to make it flaky.
	fsys.Add(faultfs.Rule{
		Op:    faultfs.OpReadDir,
		Match: func(c faultfs.Call) bool { return c.Name == doomed },
		Times: 1,
		Before: func(faultfs.Call) {
			require.NoError(t, fsys.Remove(doomed+"/manifest"))
			require.NoError(t, fsys.Remove(doomed))
		},
	})

	keys, err := b.List(ctx, "t1/traces/")
	require.NoError(t, err)
	require.Equal(t, []string{"t1/traces/01M22YPD9JSHCNTBC1ABBM90G1/manifest"}, keys)
}

// TestWriteSurvivesConcurrentDirectoryCreator is the race where the writer that finds a directory
// already made is not the one that syncs its entry: B publishes under a tenant directory A created,
// and a power cut before A syncs the root takes B's acknowledged object with it.
func TestWriteSurvivesConcurrentDirectoryCreator(t *testing.T) {
	t.Parallel()

	const (
		creator = "t1/logs/0001/manifest"
		key     = "t1/metrics/0001/manifest"
	)

	ctx := context.Background()
	gate := faultfs.NewGate()
	fsys := faultfs.New()
	fsys.Add(gate.Rule(faultfs.OpRename, func(c faultfs.Call) bool { return c.To == creator }))

	b := newFS(fsys)

	done := make(chan error, 1)
	go func() { done <- b.Write(ctx, creator, []byte("a")) }()

	gate.Await(t)
	require.NoError(t, b.Write(ctx, key, []byte("b")))

	after := newFS(fsys.Crash())

	gate.Release()
	require.NoError(t, <-done)

	keys, err := after.List(ctx, "")
	require.NoError(t, err)
	assert.Contains(t, keys, key)
}

// TestWriteAfterPrunedDirectoriesSurvivesPowerLoss recreates directories a delete pruned: a
// directory remembered as synced before the prune is a new, unsynced one after it.
func TestWriteAfterPrunedDirectoriesSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	const key = "t1/metrics/0001/manifest"

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	require.NoError(t, b.Write(ctx, key, []byte("m")))
	require.NoError(t, b.Delete(ctx, key))
	require.NoError(t, b.Write(ctx, key, []byte("m")))

	keys, err := newFS(fsys.Crash()).List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{key}, keys)
}

// deferredPart writes a part the way the engines do: every object deferred, the manifest last.
func deferredPart(t *testing.T, b *File, prefix string) []string {
	t.Helper()

	ctx := context.Background()
	keys := []string{prefix + "/c/0", prefix + "/c/1", prefix + "/marks", prefix + "/manifest", prefix + "/smax"}

	for _, k := range keys {
		require.NoError(t, b.WriteDeferred(ctx, k, []byte(k)))
	}

	slices.Sort(keys)

	return keys
}

func syncDirs(calls []faultfs.Call) []string {
	var out []string

	for _, c := range calls {
		if c.Op == faultfs.OpSyncDir {
			out = append(out, c.Name)
		}
	}

	return out
}

func countOps(calls []faultfs.Call, op faultfs.Op) int {
	var n int

	for _, c := range calls {
		if c.Op == op {
			n++
		}
	}

	return n
}

func TestSyncPrefixMakesDeferredPartDurable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	keys := deferredPart(t, b, "t1/metrics/0001")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))

	got, err := newFS(fsys.Crash()).List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, keys, got)
}

// TestSyncPrefixNeverExposesPartialPart crashes at every directory sync SyncPrefix makes: what
// survives is either none of the part or all of it, never a manifest without what it names.
func TestSyncPrefixNeverExposesPartialPart(t *testing.T) {
	t.Parallel()

	const part = "t1/metrics/0001"

	for step := range 5 {
		t.Run(strconv.Itoa(step), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			fsys := faultfs.New()
			b := newFS(fsys)
			keys := deferredPart(t, b, part)

			gate := faultfs.NewGate()
			seen := 0
			fsys.Add(faultfs.Rule{Op: faultfs.OpSyncDir, Before: func(c faultfs.Call) {
				seen++
				if seen == step+1 {
					gate.Rule(faultfs.OpSyncDir, nil).Before(c)
				}
			}})

			done := make(chan error, 1)
			go func() { done <- b.SyncPrefix(ctx, part) }()

			held := gate.Await(t)
			after := newFS(fsys.Crash())

			gate.Release()
			require.NoError(t, <-done)

			got, err := after.List(ctx, "")
			require.NoError(t, err)

			if len(got) > 0 {
				assert.Equal(t, keys, got, "crash before syncing %q", held.Name)
			}
		})
	}
}

// TestDeferredPartSyncCount pins the saving: a part costs a file fsync per object and, once its
// engine directory is known durable, three directory syncs however many objects it has.
func TestDeferredPartSyncCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	deferredPart(t, b, "t1/metrics/0001")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))

	before := len(fsys.Calls())
	keys := deferredPart(t, b, "t1/metrics/0002")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0002"))

	calls := fsys.Calls()[before:]
	assert.Equal(t, len(keys), countOps(calls, faultfs.OpSync))
	assert.Equal(t, []string{"t1/metrics/0002/c", "t1/metrics/0002", "t1/metrics"}, syncDirs(calls))
}

func TestDeferredDeleteSyncCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	keys := deferredPart(t, b, "t1/metrics/0001")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))
	require.NoError(t, b.Write(ctx, "t1/metrics/index", []byte("i")))

	before := len(fsys.Calls())
	require.NoError(t, b.Delete(ctx, "t1/metrics/0001/manifest"))

	for _, k := range keys {
		if k != "t1/metrics/0001/manifest" {
			require.NoError(t, b.DeleteDeferred(ctx, k))
		}
	}

	assert.Equal(t, []string{"t1/metrics/0001"}, syncDirs(fsys.Calls()[before:]))

	got, err := newFS(fsys.Crash()).List(ctx, "")
	require.NoError(t, err)
	assert.NotContains(t, got, "t1/metrics/0001/manifest")
}

// TestDeferredDeleteForgetsPrunedDirectories rewrites a part a deferred delete pruned: the
// directories are new, and SyncPrefix must not trust what it remembered about the old ones.
func TestDeferredDeleteForgetsPrunedDirectories(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	keys := deferredPart(t, b, "t1/metrics/0001")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))

	for _, k := range keys {
		require.NoError(t, b.DeleteDeferred(ctx, k))
	}

	deferredPart(t, b, "t1/metrics/0001")
	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))

	got, err := newFS(fsys.Crash()).List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, keys, got)
}

func TestCreateObjectDeferredWaitsForSyncPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fsys := faultfs.New()
	b := newFS(fsys)

	w, err := b.CreateObjectDeferred(ctx, "t1/metrics/0001/c/0")
	require.NoError(t, err)
	_, err = w.Write([]byte("column"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(ctx))

	assert.Empty(t, syncDirs(fsys.Calls()))

	require.NoError(t, b.SyncPrefix(ctx, "t1/metrics/0001"))
	assert.Equal(t, []byte("column"), read(t, newFS(fsys.Crash()), "t1/metrics/0001/c/0"))
}

func TestSyncPrefixOfMissingPrefix(t *testing.T) {
	t.Parallel()

	assert.NoError(t, newFS(faultfs.New()).SyncPrefix(context.Background(), "t1/metrics/0001"))
}

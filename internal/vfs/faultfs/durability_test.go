package faultfs_test

import (
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// publish writes body to a temp name, syncs it and renames it into place — the sequence the file
// backend uses, deliberately without the directory sync, so the tests below can add it or not.
func publish(t *testing.T, f vfs.FS, tmp, name, body string) {
	t.Helper()

	w, err := f.OpenFile(tmp, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.NoError(t, f.Rename(tmp, name))
}

// TestCrashLosesUnsyncedWrite is the first of the two promises: bytes reach the disk on Sync, and a
// power cut takes what has not been synced.
func TestCrashLosesUnsyncedWrite(t *testing.T) {
	t.Parallel()

	f := faultfs.New()

	w, err := f.OpenFile("obj", os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write([]byte("written"))
	require.NoError(t, err)
	require.NoError(t, w.Close()) // closing commits nothing
	require.NoError(t, f.SyncDir("."))

	after := f.Crash()

	got, err := after.ReadFile("obj")
	require.NoError(t, err, "the directory sync published the name")
	assert.Empty(t, got, "but the bytes were never synced, so the file comes back empty")
}

// TestCrashLosesUnsyncedName is the second promise, and the one #480 is about: fsyncing the file
// commits its bytes and says nothing about the name that reaches them.
func TestCrashLosesUnsyncedName(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	publish(t, f, ".tmp-obj", "obj", "durable-bytes")

	// Everything a careful writer does short of syncing the directory.
	got, err := f.ReadFile("obj")
	require.NoError(t, err)
	require.Equal(t, []byte("durable-bytes"), got, "visible before the crash")

	after := f.Crash()

	_, err = after.OpenFile("obj", os.O_RDONLY, 0)
	assert.ErrorIs(t, err, fs.ErrNotExist,
		"the bytes were synced but the rename was not, so nothing names them")
}

// TestSyncDirMakesPublishDurable is the same sequence done right.
func TestSyncDirMakesPublishDurable(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	publish(t, f, ".tmp-obj", "obj", "durable-bytes")
	require.NoError(t, f.SyncDir("."))

	got, ok := f.Durable("obj")
	require.True(t, ok)
	assert.Equal(t, []byte("durable-bytes"), got)

	after := f.Crash()
	got, err := after.ReadFile("obj")
	require.NoError(t, err)
	assert.Equal(t, []byte("durable-bytes"), got)
}

// TestCrashUndoesUnsyncedRemove: an unlink is a directory change like any other, so a removal that
// was not committed comes back.
func TestCrashUndoesUnsyncedRemove(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	publish(t, f, ".tmp-obj", "obj", "body")
	require.NoError(t, f.SyncDir("."))

	require.NoError(t, f.Remove("obj"))

	after := f.Crash()
	got, err := after.ReadFile("obj")
	require.NoError(t, err, "the unlink was never committed")
	assert.Equal(t, []byte("body"), got)

	// Committed, it stays gone.
	require.NoError(t, f.SyncDir("."))
	_, err = f.Crash().OpenFile("obj", os.O_RDONLY, 0)
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// TestKillKeepsEverything separates the two failure modes: a dead process leaves its writes in the
// page cache, so code that is safe against SIGKILL is not thereby safe against a power cut.
func TestKillKeepsEverything(t *testing.T) {
	t.Parallel()

	f := faultfs.New()

	w, err := f.OpenFile("obj", os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write([]byte("never-synced"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	got, err := f.Kill().ReadFile("obj")
	require.NoError(t, err)
	assert.Equal(t, []byte("never-synced"), got, "the kernel still owned these bytes")

	_, err = f.Crash().OpenFile("obj", os.O_RDONLY, 0)
	assert.ErrorIs(t, err, fs.ErrNotExist, "a power cut did not")
}

// TestTearKeepsAPrefix models the partial write a torn append leaves behind — the shape a WAL
// replayer must tell apart from a clean end of file.
func TestTearKeepsAPrefix(t *testing.T) {
	t.Parallel()

	f := faultfs.New()

	w, err := f.OpenFile("seg", os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write([]byte("committed;"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, f.SyncDir("."))

	_, err = w.Write([]byte("torn-record"))
	require.NoError(t, err)
	require.NoError(t, f.Tear("seg", 4)) // four bytes of the uncommitted tail reached the platter

	got, err := f.Crash().ReadFile("seg")
	require.NoError(t, err)
	assert.Equal(t, []byte("committed;torn"), got)
}

// TestRuleFailsMatchingOp: the injected-error half of the package, in faultbackend's shape.
func TestRuleFailsMatchingOp(t *testing.T) {
	t.Parallel()

	injected := errors.New("no space left on device")
	f := faultfs.New()
	f.Add(faultfs.Rule{Op: faultfs.OpSync, Err: injected, Times: 1})

	w, err := f.OpenFile("obj", os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = w.Write([]byte("x"))
	require.NoError(t, err)

	require.ErrorIs(t, w.Sync(), injected)
	assert.NoError(t, w.Sync(), "Times bounds the rule to one operation")
}

// TestGateSuspendsUntilReleased: a crash can be placed *inside* a publish, between the rename and
// the directory sync that would have committed it.
func TestGateSuspendsUntilReleased(t *testing.T) {
	t.Parallel()

	f := faultfs.New()
	gate := faultfs.NewGate()
	f.Add(gate.Rule(faultfs.OpSyncDir, nil))

	var wg sync.WaitGroup

	wg.Go(func() {
		publish(t, f, ".tmp-obj", "obj", "body")
		_ = f.SyncDir(".")
	})

	gate.Await(t) // the rename happened; the directory sync has not

	_, err := f.Crash().OpenFile("obj", os.O_RDONLY, 0)
	require.ErrorIs(t, err, fs.ErrNotExist, "crashing here loses the publish")

	gate.Release()
	wg.Wait()

	_, err = f.Crash().ReadFile("obj")
	assert.NoError(t, err, "and completing the sync makes it durable")
}

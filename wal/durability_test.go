package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// sides replays fsys and returns the opaque side payloads in order, with the replay error.
func sides(fsys vfs.FS) ([]string, error) {
	var got []string

	err := replayDirFrom(fsys, 0, Handlers{
		OnSide: func(payload []byte) error { got = append(got, string(payload)); return nil },
	})

	return got, err
}

// TestSyncAlwaysSurvivesPowerLoss: with the per-record fsync policy on, every acknowledged record
// comes back from a power cut — including the ones in segments opened after a rotation, whose
// directory entries are durable only because the rotation syncs the directory.
func TestSyncAlwaysSurvivesPowerLoss(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 10) // tiny limit ⇒ a rotation every couple of records
	require.NoError(t, err)
	w.SetSync(true)

	want := []string{"a", "b", "c", "d"}
	for _, p := range want {
		require.NoError(t, w.WriteSide([]byte(p)))
	}

	require.Greater(t, w.Seq(), 1, "the run rotated")

	got, err := sides(fsys.Crash())
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestSyncNoneSurvivesProcessDeath states the other half of the policy honestly: without the
// per-record fsync the records survive a dead *process*, and a dead machine takes them.
func TestSyncNoneSurvivesProcessDeath(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("a")))

	got, err := sides(fsys.Kill())
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, got, "the page cache outlives the process")

	got, err = sides(fsys.Crash())
	require.NoError(t, err)
	assert.Empty(t, got, "a power cut takes what was never synced")
}

// TestCheckpointRemovalsSurvivePowerLoss: a checkpoint's deletions are durable, so a power cut
// cannot resurrect segments a flushed part supersedes and have replay re-apply them.
func TestCheckpointRemovalsSurvivePowerLoss(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	w.SetSync(true)
	require.NoError(t, w.WriteSide([]byte("flushed")))
	require.NoError(t, w.Checkpoint())

	// The cut lands between the checkpoint and the next segment, so nothing else syncs the
	// directory on this run's behalf.
	got, err := sides(fsys.Crash())
	require.NoError(t, err)
	assert.Empty(t, got, "the superseded segment does not come back")

	require.NoError(t, w.WriteSide([]byte("live")))

	got, err = sides(fsys.Crash())
	require.NoError(t, err)
	assert.Equal(t, []string{"live"}, got)
}

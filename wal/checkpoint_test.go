package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// collectSides replays dir and returns the opaque side payloads in order.
func collectSides(t *testing.T, dir string) []string {
	t.Helper()

	var got []string
	require.NoError(t, ReplayDir(dir, Handlers{
		OnSide: func(payload []byte) error { got = append(got, string(payload)); return nil },
	}))

	return got
}

// TestSegmentResume verifies a reopened writer appends beyond the prior run's segments rather than
// truncating them, so a replay still sees the earlier records.
func TestSegmentResume(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	w1, err := Create(dir, 0)
	require.NoError(t, err)
	require.NoError(t, w1.WriteSide([]byte("first")))
	require.NoError(t, w1.Close())

	w2, err := Create(dir, 0) // resume
	require.NoError(t, err)
	require.NoError(t, w2.WriteSide([]byte("second")))
	require.NoError(t, w2.Close())

	require.Equal(t, []string{"first", "second"}, collectSides(t, dir))
}

// TestSegmentCheckpoint verifies a checkpoint discards the records written before it, so a replay
// sees only what followed.
func TestSegmentCheckpoint(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	w, err := Create(dir, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("flushed-a")))
	require.NoError(t, w.WriteSide([]byte("flushed-b")))

	require.NoError(t, w.Checkpoint()) // both above are now durable elsewhere

	require.NoError(t, w.WriteSide([]byte("live")))
	require.NoError(t, w.Close())

	require.Equal(t, []string{"live"}, collectSides(t, dir))
}

// TestCheckpointThenResume verifies resume after a checkpoint keeps only the post-checkpoint records
// across a reopen.
func TestCheckpointThenResume(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	w, err := Create(dir, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("old")))
	require.NoError(t, w.Checkpoint())
	require.NoError(t, w.WriteSide([]byte("kept")))
	require.NoError(t, w.Close())

	w2, err := Create(dir, 0)
	require.NoError(t, err)
	require.NoError(t, w2.WriteSide([]byte("new")))
	require.NoError(t, w2.Close())

	require.Equal(t, []string{"kept", "new"}, collectSides(t, dir))
}

package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/faultfs"
)

// writeSeg puts data in fsys under the segment name for seq, durably.
func writeSeg(tb testing.TB, fsys vfs.FS, seq int, data []byte) {
	tb.Helper()

	f, err := fsys.OpenFile(segmentName(seq, 1), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(tb, err)
	_, err = f.Write(data)
	require.NoError(tb, err)
	require.NoError(tb, f.Sync())
	require.NoError(tb, f.Close())
	require.NoError(tb, fsys.SyncDir("."))
}

// TestTornNonFinalSegment: a segment that ends mid-frame while later segments exist is a hole in the
// middle of history. Replay must refuse it rather than skip to the next segment.
func TestTornNonFinalSegment(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	first := appendFrame(nil, recordSide, []byte("seg1-rec1"))
	full := len(first)
	first = appendFrame(first, recordSide, []byte("seg1-rec2"))

	writeSeg(t, fsys, 1, first[:full+2]) // frame 2 of segment 1 reached the platter half-written
	writeSeg(t, fsys, 2, appendFrame(nil, recordSide, []byte("seg2-rec1")))

	got, err := sides(fsys)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Equal(t, []string{"seg1-rec1"}, got, "the records before the tear are still applied")
}

// TestTornFinalSegmentTolerated: the same tear in the *last* segment is ordinary crash recovery.
func TestTornFinalSegmentTolerated(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	writeSeg(t, fsys, 1, appendFrame(nil, recordSide, []byte("seg1-rec1")))

	last := appendFrame(nil, recordSide, []byte("seg2-rec1"))
	full := len(last)
	last = appendFrame(last, recordSide, []byte("seg2-rec2"))
	writeSeg(t, fsys, 2, last[:full+2])

	got, err := sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"seg1-rec1", "seg2-rec1"}, got)
}

// TestRepairOnResume walks the whole crash-restart-crash sequence: a torn tail left by a power cut
// is repaired when the writer resumes, so it never becomes a middle segment — and a second restart
// over the repaired directory is clean.
func TestRepairOnResume(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	w.SetSync(true)
	require.NoError(t, w.WriteSide([]byte("committed")))

	// The next record's bytes are in flight when the machine loses power: two of them reach the
	// platter, the rest do not.
	w.SetSync(false)
	require.NoError(t, w.WriteSide([]byte("in-flight")))
	require.NoError(t, fsys.Tear(segmentName(1, 1), 2))

	crashed := fsys.Crash()

	got, err := sides(crashed)
	require.NoError(t, err, "a tear in the only (last) segment is crash recovery")
	require.Equal(t, []string{"committed"}, got)

	w2, err := createFS(crashed, 0) // resume: repairs the torn tail before opening segment 2
	require.NoError(t, err)
	require.NoError(t, w2.WriteSide([]byte("after-restart")))
	require.NoError(t, w2.Close())

	got, err = sides(crashed)
	require.NoError(t, err, "the repaired segment 1 is a clean middle segment")
	require.Equal(t, []string{"committed", "after-restart"}, got)

	w3, err := createFS(crashed, 0) // and a second restart over the repaired directory
	require.NoError(t, err)
	require.NoError(t, w3.WriteSide([]byte("after-second-restart")))
	require.NoError(t, w3.Close())

	got, err = sides(crashed)
	require.NoError(t, err)
	require.Equal(t, []string{"committed", "after-restart", "after-second-restart"}, got)
}

// TestRepairLeavesCorruptSegment: repair truncates a torn tail, never a complete frame that fails
// its CRC — that is corruption, and replay stays the one place that reports it.
func TestRepairLeavesCorruptSegment(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	data := appendFrame(nil, recordSide, []byte("rec1"))
	data = appendFrame(data, recordSide, []byte("rec2"))
	data[len(data)-1] ^= 0xFF // a bad CRC on the final frame
	writeSeg(t, fsys, 1, data)

	_, err := createFS(fsys, 0)
	require.NoError(t, err)

	kept, err := fsys.ReadFile(segmentName(1, 1))
	require.NoError(t, err)
	assert.Equal(t, data, kept, "the corrupt frame is left in place")

	_, err = sides(fsys)
	require.ErrorIs(t, err, ErrCorrupt)
}

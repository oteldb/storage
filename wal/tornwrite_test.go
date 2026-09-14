package wal

import (
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
)

var errDiskFull = errors.New("injected: no space left on device")

// failNextSegmentWrite makes the next write to a segment fail after landing short bytes of it.
func failNextSegmentWrite(fsys *faultfs.FS, short int) {
	fsys.Add(faultfs.Rule{
		Op:    faultfs.OpWrite,
		Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, segmentExt) },
		Err:   errDiskFull,
		Short: short,
		Times: 1,
	})
}

// sideFrameLen is the framed size of a side record with payload p.
func sideFrameLen(p string) int { return len(appendFrame(nil, recordSide, []byte(p))) }

// TestShortWriteDoesNotCorruptLaterRecords is #616: a write that fails part-way must not leave its
// partial frame in front of the records written after it.
func TestShortWriteDoesNotCorruptLaterRecords(t *testing.T) {
	t.Parallel()

	for _, sync := range []bool{false, true} {
		t.Run(map[bool]string{false: "NoSync", true: "SyncAlways"}[sync], func(t *testing.T) {
			t.Parallel()

			fsys := faultfs.New()

			w, err := createFS(fsys, 0)
			require.NoError(t, err)
			w.SetSync(sync)

			require.NoError(t, w.WriteSide([]byte("committed")))

			failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
			require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)

			require.NoError(t, w.WriteSide([]byte("after")))
			require.NoError(t, w.Close())

			got, err := sides(fsys)
			require.NoError(t, err, "a short write must not become corruption for the records behind it")
			assert.Equal(t, []string{"committed", "after"}, got)

			resumed, err := createFS(fsys, 0)
			require.NoError(t, err)
			require.NoError(t, resumed.Close())
		})
	}
}

// TestFailedWriteThatLandedNothingKeepsItsSegment: a write that fails before landing a byte leaves no
// tear, so there is nothing to restore and the writer keeps appending to the same segment.
func TestFailedWriteThatLandedNothingKeepsItsSegment(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))

	seq := w.Seq()

	failNextSegmentWrite(fsys, 0)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)
	require.NoError(t, w.WriteSide([]byte("after")))

	assert.Equal(t, seq, w.Seq(), "no segment was opened")
	require.NoError(t, w.Close())

	got, err := sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"committed", "after"}, got)
}

// TestUnhealedTearRefusesWrites: while the torn segment cannot be restored, nothing is written behind
// it — not even into a new segment, which would put the tear in the middle of the log — and the
// directory stays recoverable. The next write after the disk recovers heals and goes through.
func TestUnhealedTearRefusesWrites(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))

	errRename := errors.New("injected rename failure")
	fsys.Add(faultfs.Rule{Op: faultfs.OpRename, Err: errRename, Times: 2})

	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)

	seq := w.Seq()

	require.ErrorIs(t, w.WriteSide([]byte("refused")), errRename)
	assert.Equal(t, seq, w.Seq(), "no segment is opened behind an unhealed tear")

	got, err := sides(fsys)
	require.NoError(t, err, "the tear is still the log's tail, which replay tolerates")
	assert.Equal(t, []string{"committed"}, got)

	require.NoError(t, w.WriteSide([]byte("after")))
	require.NoError(t, w.Close())

	got, err = sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"committed", "after"}, got)
}

// TestCrashWhileHealingKeepsAcknowledgedRecords: the segment is restored by renaming a copy over it,
// so a power cut at any point of the restore keeps every record synced before the failed write.
func TestCrashWhileHealingKeepsAcknowledgedRecords(t *testing.T) {
	t.Parallel()

	for _, unsynced := range []int{0, 100} {
		for _, op := range []faultfs.Op{faultfs.OpCreate, faultfs.OpWrite, faultfs.OpSync, faultfs.OpRename, faultfs.OpSyncDir} {
			t.Run(fmt.Sprintf("%s/unsynced%d", op, unsynced), func(t *testing.T) {
				t.Parallel()

				fsys := faultfs.New()

				w, err := createFS(fsys, 0)
				require.NoError(t, err)
				w.SetSync(true)
				require.NoError(t, w.WriteSide([]byte("committed")))

				failNextSegmentWrite(fsys, sideFrameLen("lost")/2)

				var crashed *faultfs.FS

				fsys.Add(faultfs.Rule{
					Op:    op,
					Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".repair") || c.Op == faultfs.OpSyncDir },
					Before: func(faultfs.Call) {
						if crashed == nil {
							crashed = fsys.CrashWith(faultfs.CrashConfig{UnsyncedPercent: unsynced})
						}
					},
				})

				require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)
				require.NotNil(t, crashed, "the crash point was never reached")

				resumed, err := createFS(crashed, 0)
				require.NoError(t, err)
				require.NoError(t, resumed.Close())

				got, err := sides(crashed)
				require.NoError(t, err)
				assert.Equal(t, []string{"committed"}, got)
			})
		}
	}
}

// TestTornSegmentCheckpointedBeforeHeal: a torn segment whose records a flush superseded is gone by the
// time the writer heals, which leaves nothing to restore.
func TestTornSegmentCheckpointedBeforeHeal(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("flushed")))

	fsys.Add(faultfs.Rule{Op: faultfs.OpRename, Err: errors.New("injected rename failure"), Times: 1})
	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)

	require.NoError(t, w.Checkpoint())
	require.NoError(t, w.WriteSide([]byte("next")))
	require.NoError(t, w.Close())

	got, err := sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"next"}, got)
}

// TestHealRetriesDirSyncAfterRename: a restore whose rename landed but whose directory sync failed is
// not healed — the rename may not have reached the disk — so the retry syncs the directory before any
// segment is opened behind the torn one.
func TestHealRetriesDirSyncAfterRename(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))

	errDirSync := errors.New("injected directory sync failure")
	fsys.Add(faultfs.Rule{Op: faultfs.OpSyncDir, Err: errDirSync, Times: 1})

	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)
	require.NoError(t, w.WriteSide([]byte("after")))
	require.NoError(t, w.Close())

	calls := fsys.Calls()
	renamed := slices.IndexFunc(calls, func(c faultfs.Call) bool { return c.Op == faultfs.OpRename })
	require.NotEqual(t, -1, renamed)

	opened := slices.IndexFunc(calls[renamed:], func(c faultfs.Call) bool {
		return c.Op == faultfs.OpCreate && strings.HasSuffix(c.Name, segmentExt)
	})
	require.NotEqual(t, -1, opened)

	syncs := 0
	for _, c := range calls[renamed : renamed+opened] {
		if c.Op == faultfs.OpSyncDir {
			syncs++
		}
	}

	assert.Equal(t, 2, syncs, "the failed directory sync is retried before the next segment is opened")

	got, err := sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"committed", "after"}, got)
}

// TestHealAfterSetEpochReleasesTornSegment: a flush that moves the epoch between a failed restore and
// its retry must not stop heal from closing the torn segment's handle. On POSIX a handle left open
// would take the writes after the rename into the replaced file's orphaned bytes; on Windows the rename
// over it fails and the writer never heals.
func TestHealAfterSetEpochReleasesTornSegment(t *testing.T) {
	t.Parallel()

	for _, windows := range []bool{false, true} {
		t.Run(map[bool]string{false: "POSIX", true: "Windows"}[windows], func(t *testing.T) {
			t.Parallel()

			fsys := faultfs.New()
			if windows {
				fsys.LockOpenFiles()
			}

			w, err := createFS(fsys, 0)
			require.NoError(t, err)
			require.NoError(t, w.WriteSide([]byte("committed")))

			// The restore fails reading the torn segment, before it gets to release the handle.
			fsys.Add(faultfs.Rule{
				Op:    faultfs.OpRead,
				Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, segmentExt) },
				Err:   errors.New("injected read failure"),
				Times: 1,
			})
			failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
			require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)

			w.SetEpoch(2)

			require.NoError(t, w.WriteSide([]byte("after")))
			require.NoError(t, w.Close())

			got, err := sides(fsys)
			require.NoError(t, err)
			assert.Equal(t, []string{"committed", "after"}, got)
		})
	}
}

// TestCreateRemovesRepairTemps: a restore interrupted by a crash leaves its temporary file behind;
// the next writer removes it, so repeated crashes do not accumulate them.
func TestCreateRemovesRepairTemps(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))
	require.NoError(t, w.Close())

	tmp := segmentName(1, 1) + repairExt
	f, err := fsys.OpenFile(tmp, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	resumed, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, resumed.Close())

	_, err = fsys.Stat(tmp)
	require.ErrorIs(t, err, fs.ErrNotExist)

	got, err := sides(fsys)
	require.NoError(t, err)
	assert.Equal(t, []string{"committed"}, got)
}

// failRepairWrites makes every write of a restore's temporary copy fail, the full disk that tore the
// segment still refusing the space for its restore.
func failRepairWrites(fsys *faultfs.FS) {
	fsys.Add(faultfs.Rule{
		Op:    faultfs.OpWrite,
		Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, repairExt) },
		Err:   errDiskFull,
	})
}

// TestUnhealedTearFailsSyncAndClose: the records written before a failed write become durable only when
// the torn segment is restored, so Sync and Close report failure until it is, rather than succeeding on
// a writer whose segment handle the restore already let go of.
func TestUnhealedTearFailsSyncAndClose(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))

	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	failRepairWrites(fsys)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)

	require.ErrorIs(t, w.Sync(), errDiskFull)

	fsys.Reset()
	require.NoError(t, w.Sync())

	got, err := sides(fsys.Crash())
	require.NoError(t, err)
	assert.Equal(t, []string{"committed"}, got, "a Sync that succeeded made the records durable")

	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	failRepairWrites(fsys)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)
	require.ErrorIs(t, w.Close(), errDiskFull)
}

// TestSyncFailsUntilRestoreIsDurable: a restore whose rename landed but whose directory sync keeps
// failing is not durable, so Sync keeps failing until the directory sync succeeds.
func TestSyncFailsUntilRestoreIsDurable(t *testing.T) {
	t.Parallel()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteSide([]byte("committed")))

	errDirSync := errors.New("injected directory sync failure")
	fsys.Add(faultfs.Rule{Op: faultfs.OpSyncDir, Err: errDirSync, Times: 2})

	failNextSegmentWrite(fsys, sideFrameLen("lost")/2)
	require.ErrorIs(t, w.WriteSide([]byte("lost")), errDiskFull)
	require.ErrorIs(t, w.Sync(), errDirSync)
	require.NoError(t, w.Sync())

	got, err := sides(fsys.Crash())
	require.NoError(t, err)
	assert.Equal(t, []string{"committed"}, got)
}

// tornDirectory returns a directory whose last segment holds synced records a and b followed by a torn
// frame, as a process that died mid-append leaves it.
func tornDirectory(t *testing.T) *faultfs.FS {
	t.Helper()

	fsys := faultfs.New()

	w, err := createFS(fsys, 0)
	require.NoError(t, err)
	w.SetSync(true)
	require.NoError(t, w.WriteSide([]byte("a")))
	require.NoError(t, w.WriteSide([]byte("b")))

	frame := appendFrame(nil, recordSide, []byte("torn"))
	_, err = w.f.Write(frame[:len(frame)/2])
	require.NoError(t, err)
	require.NoError(t, w.f.Sync())

	return fsys.Kill()
}

// TestCreateRepairFailureKeepsRecords is #617: a startup repair whose rewrite fails must leave the
// segment's records in place for the next attempt.
func TestCreateRepairFailureKeepsRecords(t *testing.T) {
	t.Parallel()

	for _, op := range []faultfs.Op{faultfs.OpWrite, faultfs.OpSync, faultfs.OpRename, faultfs.OpSyncDir} {
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()

			fsys := tornDirectory(t)
			fsys.Add(faultfs.Rule{Op: op, Err: errDiskFull, Times: 1})

			_, err := createFS(fsys, 0)
			require.ErrorIs(t, err, errDiskFull)

			resumed, err := createFS(fsys, 0)
			require.NoError(t, err)
			require.NoError(t, resumed.Close())

			got, err := sides(fsys)
			require.NoError(t, err)
			assert.Equal(t, []string{"a", "b"}, got)
		})
	}
}

// TestCrashDuringCreateRepairKeepsRecords: a power cut at any step of a startup repair keeps the
// segment's synced records, and the next start repairs it again.
func TestCrashDuringCreateRepairKeepsRecords(t *testing.T) {
	t.Parallel()

	for _, unsynced := range []int{0, 100} {
		for _, op := range []faultfs.Op{faultfs.OpCreate, faultfs.OpWrite, faultfs.OpSync, faultfs.OpRename, faultfs.OpSyncDir} {
			t.Run(fmt.Sprintf("%s/unsynced%d", op, unsynced), func(t *testing.T) {
				t.Parallel()

				fsys := tornDirectory(t)

				var crashed *faultfs.FS

				fsys.Add(faultfs.Rule{
					Op: op,
					Before: func(faultfs.Call) {
						if crashed == nil {
							crashed = fsys.CrashWith(faultfs.CrashConfig{UnsyncedPercent: unsynced})
						}
					},
				})

				resumed, err := createFS(fsys, 0)
				require.NoError(t, err)
				require.NoError(t, resumed.Close())
				require.NotNil(t, crashed, "the crash point was never reached")

				again, err := createFS(crashed, 0)
				require.NoError(t, err)
				require.NoError(t, again.Close())

				got, err := sides(crashed)
				require.NoError(t, err)
				assert.Equal(t, []string{"a", "b"}, got)
			})
		}
	}
}

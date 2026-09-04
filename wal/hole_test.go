package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/signal"
)

// holedLog returns a three-record log whose second record is overwritten with zeros — the shape
// per-block writeback leaves when a later block reaches the platter and an earlier one does not.
func holedLog(t *testing.T) []byte {
	t.Helper()

	s := mkSeries("job", "api")
	id := s.Hash()

	var buf bytes.Buffer

	w := NewWriter(&buf)
	require.NoError(t, w.WriteSeries(id, s))

	stop := buf.Len()

	require.NoError(t, w.WriteSamples(id, []int64{1, 2}, []float64{1, 2}))
	require.NoError(t, w.WriteSide([]byte("survivor")))

	data := buf.Bytes()
	require.Greater(t, len(data), stop+8)
	clear(data[stop : stop+8])

	return data
}

func writeSegmentFile(t *testing.T, dir string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, segmentName(1, 1)), data, 0o600))
}

func TestReplayDirHoleInFinalSegment(t *testing.T) {
	t.Parallel()

	data := holedLog(t)

	require.ErrorIs(t, Replay(data, Handlers{}), ErrCorrupt, "whole-buffer replay already rejects it")

	dir := t.TempDir()
	writeSegmentFile(t, dir, data)

	var seen int

	err := ReplayDir(dir, Handlers{OnSide: func([]byte) error { seen++; return nil }})
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "hole in final segment")
	assert.Zero(t, seen, "the records past the hole are not applied as if the log ended")
}

func TestCreateRefusesHole(t *testing.T) {
	t.Parallel()

	data := holedLog(t)

	dir := t.TempDir()
	writeSegmentFile(t, dir, data)

	// Create repairs the last segment before anything replays it; truncating a hole would erase the
	// records past it and leave replay a clean-looking prefix.
	_, err := Create(dir, 0)
	require.ErrorIs(t, err, ErrCorrupt)

	after, rerr := os.ReadFile(filepath.Join(dir, segmentName(1, 1)))
	require.NoError(t, rerr)
	assert.Equal(t, data, after, "the segment is left intact for an operator to inspect")
}

func TestReplayDirTornTailStillAccepted(t *testing.T) {
	t.Parallel()

	s := mkSeries("job", "api")
	id := s.Hash()

	var buf bytes.Buffer

	w := NewWriter(&buf)
	require.NoError(t, w.WriteSeries(id, s))
	require.NoError(t, w.WriteSide([]byte("partially appended")))

	data := buf.Bytes()[:buf.Len()-5]

	dir := t.TempDir()
	writeSegmentFile(t, dir, data)

	var seen int

	require.NoError(t, ReplayDir(dir, Handlers{OnSeries: func(signal.SeriesID, signal.Series) error {
		seen++

		return nil
	}}))
	assert.Equal(t, 1, seen)
}

func TestFrameAfter(t *testing.T) {
	t.Parallel()

	frame := appendFrame(nil, recordSide, []byte("payload"))

	assert.False(t, frameAfter(nil))
	assert.False(t, frameAfter(make([]byte, 4096)), "a zero-filled region holds no frame")
	assert.False(t, frameAfter(frame[:len(frame)-1]), "a torn frame is not a frame")
	assert.True(t, frameAfter(frame))
	assert.True(t, frameAfter(append(make([]byte, 4096), frame...)), "found past a zero-filled hole")
}

// hasFrameRef is an independent reference for [frameAfter]: it re-derives the framing and the
// checksum from the format rather than reusing readFrame, so the fuzz target's invariant does not
// rest on the implementation it checks.
func hasFrameRef(data []byte) bool {
	for off := range data {
		bodyLen, n := binary.Uvarint(data[off:])
		if n <= 0 || bodyLen == 0 || bodyLen > uint64(len(data)) {
			continue
		}

		end := off + n + int(bodyLen)
		if end+4 > len(data) {
			continue
		}

		if crc32.Checksum(data[off:end], castagnoli) == binary.BigEndian.Uint32(data[end:end+4]) {
			return true
		}
	}

	return false
}

// FuzzReplayDirHole asserts the classification a WAL directory replay makes about its last segment:
// stopping before the end of the buffer is only allowed when nothing whole follows the stopping
// point. Both directions matter — accepting a hole drops committed records silently, and rejecting a
// genuine torn tail fails recovery on a healthy log.
func FuzzReplayDirHole(f *testing.F) {
	var buf bytes.Buffer

	s := mkSeries("job", "api")
	w := NewWriter(&buf)
	_ = w.WriteSeries(s.Hash(), s)
	_ = w.WriteSamples(s.Hash(), []int64{1, 2}, []float64{1, 2})
	_ = w.WriteSide([]byte("survivor"))
	f.Add(buf.Bytes())
	f.Add(buf.Bytes()[:buf.Len()-3])
	f.Add([]byte{})

	holed := bytes.Clone(buf.Bytes())
	clear(holed[8:24])
	f.Add(holed)

	seeds, err := filepath.Glob(filepath.Join("testdata", "fuzz", "FuzzReplay", "*"))
	if err != nil {
		f.Fatal(err)
	}

	for _, name := range seeds {
		if b, rerr := os.ReadFile(name); rerr == nil {
			f.Add(b)
		}
	}

	dir := f.TempDir()
	name := segmentName(1, 1)

	f.Fuzz(func(t *testing.T, data []byte) {
		stop, err := replay(data, Handlers{})
		if err != nil {
			return // a failing CRC is reported by both paths
		}

		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}

		fsys, err := vfs.Open(dir)
		if err != nil {
			t.Fatal(err)
		}

		derr := replayDirFrom(fsys, 0, Handlers{})
		_ = fsys.Close()

		switch {
		case stop == len(data):
			if derr != nil {
				t.Fatalf("complete log rejected: %v", derr)
			}
		case hasFrameRef(data[stop:]):
			if derr == nil {
				t.Fatalf("hole at offset %d of %d accepted as a torn tail", stop, len(data))
			}
		default:
			if derr != nil {
				t.Fatalf("torn tail at offset %d of %d rejected: %v", stop, len(data), derr)
			}
		}
	})
}

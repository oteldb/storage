package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentWriterAccessors(t *testing.T) {
	t.Parallel()

	sw, err := Create(t.TempDir(), 0)
	require.NoError(t, err)
	// Close the open segment before TempDir cleanup, else Windows can't unlink the in-use .wal file.
	t.Cleanup(func() { _ = sw.Close() })

	// Before the first write no segment is open; the epoch starts at the first generation.
	assert.Equal(t, 0, sw.Seq())
	assert.Equal(t, 0, sw.Size())
	assert.Equal(t, uint64(1), sw.Epoch())

	s := mkSeries("job", "api")
	id := s.Hash()
	require.NoError(t, sw.WriteSeries(id, s))

	assert.Equal(t, 1, sw.Seq(), "first write opens segment 1")
	assert.Positive(t, sw.Size(), "segment grows with the framed record")

	before := sw.Size()
	require.NoError(t, sw.WriteSamples(id, []int64{1, 2}, []float64{10, 20}))
	assert.Greater(t, sw.Size(), before, "size tracks subsequent writes")

	// SetEpoch is reflected immediately by the accessor.
	sw.SetEpoch(7)
	assert.Equal(t, uint64(7), sw.Epoch())
}

func TestSegmentWriterOnDiskSegments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := Create(dir, 0)
	require.NoError(t, err)

	onDisk := func() (int, int64) {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)

		var size int64
		for _, e := range entries {
			// Not e.Info(): on Windows a listing carries the size NTFS last wrote to the directory
			// entry, which for the still-open segment lags its writes until the handle closes.
			info, err := os.Stat(filepath.Join(dir, e.Name()))
			require.NoError(t, err)
			size += info.Size()
		}

		return len(entries), size
	}
	assertMatchesDisk := func(msg string) {
		t.Helper()

		n, size := onDisk()
		assert.Equal(t, n, sw.Segments(), msg)
		assert.Equal(t, size, sw.Bytes(), msg)
	}

	assertMatchesDisk("empty dir")

	s := mkSeries("job", "api")
	id := s.Hash()
	write := func() {
		t.Helper()
		require.NoError(t, sw.WriteSeries(id, s))
	}

	var sealed int
	for epoch := uint64(2); epoch < 6; epoch++ {
		write()

		sealed, err = sw.Seal(epoch)
		require.NoError(t, err)
	}

	write()
	assertMatchesDisk("four sealed segments and the open one")
	assert.Equal(t, 5, sw.Segments())

	require.NoError(t, sw.CheckpointThrough(sealed))
	assertMatchesDisk("checkpoint keeps only the open segment")
	assert.Equal(t, 1, sw.Segments())

	require.NoError(t, sw.Close())
	assertMatchesDisk("closed segment stays until checkpointed")

	resumed, err := Create(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resumed.Close() })

	n, size := onDisk()
	assert.Equal(t, n, resumed.Segments(), "resume counts the prior run's segments")
	assert.Equal(t, size, resumed.Bytes())

	require.NoError(t, resumed.Checkpoint())
	assert.Zero(t, resumed.Segments())
	assert.Zero(t, resumed.Bytes())
}

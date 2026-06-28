package wal

import (
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

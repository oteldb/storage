package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnFrames pins the row→frame map a caller attributing compressed bytes to a row subset
// depends on: the extents tile [0, rows) in order, and their count follows the frame packing.
func TestColumnFrames(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 400

	granules := (n + blockRows - 1) / blockRows
	c := blockCases()[0].col(n)

	for _, tc := range []struct {
		name          string
		compressBytes int
		wantFrames    int
	}{
		{"per-granule", 1, granules},
		{"packed", defaultCompressBlockBytes, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			desc, obj, err := buildColumn(c, zstdComp(), blockRows, tc.compressBytes)
			require.NoError(t, err)

			r := newColumnReader(desc, obj, zstdComp(), n)

			frames, err := r.Frames()
			require.NoError(t, err)
			require.Len(t, frames, tc.wantFrames)

			var total int64

			row := 0

			for _, f := range frames {
				assert.Equal(t, row, f.StartRow, "extents tile in order")
				assert.Greater(t, f.EndRow, f.StartRow)
				assert.Positive(t, f.Bytes)

				row = f.EndRow
				total += f.Bytes
			}

			assert.Equal(t, n, row, "extents cover every row")
			assert.LessOrEqual(t, total, r.ObjectBytes(), "frames fit in the object, the directory is extra")
			assert.Positive(t, r.ObjectBytes())
		})
	}
}

// TestColumnFramesUnblocked: a column written as one stream has no frames to separate, so it reports
// itself as a single extent — the shape that keeps an attributing caller free of special cases.
func TestColumnFramesUnblocked(t *testing.T) {
	t.Parallel()

	const n = 64

	c := blockCases()[0].col(n)
	c.Block = false

	desc, obj, err := buildColumn(c, zstdComp(), 0, 0)
	require.NoError(t, err)
	require.False(t, desc.Blocked)

	r := newColumnReader(desc, obj, zstdComp(), n)

	frames, err := r.Frames()
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, FrameExtent{StartRow: 0, EndRow: n, Bytes: r.ObjectBytes()}, frames[0])
}

// TestColumnFramesLegacy: in the pre-framing layout every granule is its own compressed block, so
// the extents degenerate to one per granule.
func TestColumnFramesLegacy(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 26

	c := blockCases()[0].col(n)

	desc, _, err := buildColumn(c, zstdComp(), blockRows, defaultCompressBlockBytes)
	require.NoError(t, err)

	desc.Framed = false
	obj := encodeLegacyBlocked(t, c, desc.Codec, zstdComp(), blockRows)

	frames, err := newColumnReader(desc, obj, zstdComp(), n).Frames()
	require.NoError(t, err)
	require.Len(t, frames, (n+blockRows-1)/blockRows)
	assert.Equal(t, n, frames[len(frames)-1].EndRow)
}

// TestColumnFramesEmpty: no rows, no extents.
func TestColumnFramesEmpty(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(blockCases()[0].col(4), zstdComp(), 4, 1)
	require.NoError(t, err)

	frames, err := newColumnReader(desc, obj, zstdComp(), 0).Frames()
	require.NoError(t, err)
	assert.Empty(t, frames)
}

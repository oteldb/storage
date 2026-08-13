package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// splitColumns lays out one stream whose records carry bodies of the given sizes, so a test can
// state a row's width directly.
func splitColumns(t *testing.T, sizes ...int) *flushColumns {
	t.Helper()

	s := NewSchema(Column{Name: "body", Kind: KindBytes})

	c := newRecordCols(s, 0, fullSel(s))
	for i, n := range sizes {
		c.appendClone(rec{ts: int64(i + 1), bytes: [][]byte{bytes.Repeat([]byte("x"), n)}})
	}

	return buildFlushColumns(s, map[signal.SeriesID]*recordCols{{Hi: 1}: c}, nil)
}

// rowBytes must count the cells a row actually holds, since that — not the row count — is what a
// part's byte cap is spent on.
func TestRowBytes(t *testing.T) {
	t.Parallel()

	f := splitColumns(t, 10, 1000)

	// ts (8) + the stream id (16) + the body cell.
	assert.Equal(t, int64(8+streamIDBytes+10), f.rowBytes(0))
	assert.Equal(t, int64(8+streamIDBytes+1000), f.rowBytes(1))
	assert.Equal(t, f.rowBytes(0)+f.rowBytes(1), f.byteSize())
}

func TestByteRanges(t *testing.T) {
	t.Parallel()

	t.Run("unlimited is one range", func(t *testing.T) {
		t.Parallel()

		f := splitColumns(t, 100, 100, 100)
		assert.Equal(t, [][2]int{{0, 3}}, byteRanges(f, 0))
	})

	t.Run("splits on the bytes held, not the rows", func(t *testing.T) {
		t.Parallel()

		// Four 124 B rows under a 250 B cap: two rows per part, and no part over the cap. Under a
		// row cap derived from an assumed 1 KiB record these would all have landed in one part.
		f := splitColumns(t, 100, 100, 100, 100)
		assert.Equal(t, [][2]int{{0, 2}, {2, 4}}, byteRanges(f, 250))
	})

	t.Run("a fat row splits where a thin one does not", func(t *testing.T) {
		t.Parallel()

		f := splitColumns(t, 10, 1000, 10)
		assert.Equal(t, [][2]int{{0, 1}, {1, 2}, {2, 3}}, byteRanges(f, 500),
			"the oversized row takes a part of its own rather than dragging its neighbors over the cap")
	})

	t.Run("a row larger than the cap still makes progress", func(t *testing.T) {
		t.Parallel()

		f := splitColumns(t, 4096, 4096)

		ranges := byteRanges(f, 8)
		require.Len(t, ranges, 2)
		assert.Equal(t, [][2]int{{0, 1}, {1, 2}}, ranges)
	})

	t.Run("no rows", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, [][2]int{{0, 0}}, byteRanges(&flushColumns{}, 1024))
	})
}

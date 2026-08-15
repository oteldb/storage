package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// withByteColCap lowers the int32 blob bound for the duration of a test, so the overflow guards can
// be reached without allocating 2 GiB. It is a package-level var, so the test must not be parallel.
func withByteColCap(t *testing.T, limit int64) {
	t.Helper()

	prev := byteColCap
	byteColCap = limit

	t.Cleanup(func() { byteColCap = prev })
}

func capTestCols(t *testing.T, bodies ...string) *recordCols {
	t.Helper()

	c := newRecordCols(headTestSchema, len(bodies), fullSel(headTestSchema))
	for i, b := range bodies {
		c.appendClone(rec{ts: int64(100 * (i + 1)), ints: []int64{1}, bytes: [][]byte{[]byte(b)}})
	}

	return c
}

// A fetch accumulator merges the head, the in-flight flush buffer and every part a stream appears
// in, so neither headByteCap nor the part format bounds it. Overflowing byteCol's int32 offsets used
// to truncate to a negative value and surface much later as a slice-bounds panic in byteCol.at.
//
//nolint:paralleltest // mutates the package-level byteColCap
func TestAppendColsWindowRefusesOverflow(t *testing.T) {
	withByteColCap(t, 48)

	big := string(bytes.Repeat([]byte("x"), 40))

	t.Run("bulk path", func(t *testing.T) {
		acc := capTestCols(t, big)
		src := capTestCols(t, big)

		err := appendColsWindow(src, acc, 0, 1<<60)
		require.ErrorContains(t, err, "fetch result too large")
		require.ErrorContains(t, err, "body", "names the offending column")
	})

	t.Run("windowed row path", func(t *testing.T) {
		acc := capTestCols(t, big)
		src := capTestCols(t, big, big)

		// The window selects a subset, so the guard has to hold per row rather than rejecting on the
		// whole-buffer bound alone.
		err := appendColsWindow(src, acc, 150, 1<<60)
		require.ErrorContains(t, err, "fetch result too large")
	})

	t.Run("fits", func(t *testing.T) {
		acc := capTestCols(t)
		src := capTestCols(t, big)

		require.NoError(t, appendColsWindow(src, acc, 0, 1<<60))
		require.Equal(t, 1, acc.len())
	})

	t.Run("window excludes the rows that would overflow", func(t *testing.T) {
		acc := capTestCols(t, big)
		src := capTestCols(t, big, big)

		// Nothing is selected, so an accumulation that could not have fit must still succeed.
		require.NoError(t, appendColsWindow(src, acc, 1<<40, 1<<50))
		require.Equal(t, 1, acc.len())
	})
}

//nolint:paralleltest // mutates the package-level byteColCap
func TestAppendWindowRowsRefusesOverflow(t *testing.T) {
	withByteColCap(t, 48)

	big := string(bytes.Repeat([]byte("x"), 40))

	acc := capTestCols(t, big)
	src := capTestCols(t, big)

	err := appendWindowRows(acc, src, rowRange{start: 0, end: 1}, 0, 1<<60)
	require.ErrorContains(t, err, "fetch result too large")

	require.NoError(t, appendWindowRows(capTestCols(t), src, rowRange{start: 0, end: 1}, 0, 1<<60))
}

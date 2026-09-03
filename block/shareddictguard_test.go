package block

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// sharedIDObject encodes rows as a block-framed column of shared-dictionary granules carrying ids,
// so a test can place an arbitrary — including out-of-range — id at a chosen row.
func sharedIDObject(tb testing.TB, ids []int, blockRows int) []byte {
	tb.Helper()

	object, err := encodeBlockedWith(len(ids), noneComp(), blockRows, defaultCompressBlockBytes,
		func(dst []byte, lo, hi int) ([]byte, error) {
			dst = append(dst, modeShared)
			for _, id := range ids[lo:hi] {
				dst = append(dst, byte(id))
			}

			return dst, nil
		})
	require.NoError(tb, err)

	return object
}

// TestSharedDictRejectsOutOfRangeID checks that the shared-dictionary fast path bounds-checks its
// ids like the merge path does. Left unchecked, a corrupt id reaches the dictionary lookup and
// panics at [chunk.DictColumn.At] — inside the caller's row loop, far from the granule it came from.
func TestSharedDictRejectsOutOfRangeID(t *testing.T) {
	t.Parallel()

	const blockRows, n = 4, 16

	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	for _, scatter := range []bool{false, true} {
		for _, bad := range []int{0, 7, n - 1} {
			name := "gather"
			if scatter {
				name = "scatter"
			}

			t.Run(name+"/row="+itoaT(bad), func(t *testing.T) {
				t.Parallel()

				ids := make([]int, n)
				ids[bad] = len(entries) // one past the dictionary

				object := sharedIDObject(t, ids, blockRows)

				desc := ColumnDesc{
					Name: "c", Kind: KindBytes, Blocked: true, Framed: true, Checked: true, SharedDict: true,
				}

				dir, err := parseBlockDir(object, desc)
				require.NoError(t, err)

				blocks := make([]int, dir.nBlocks())
				for i := range blocks {
					blocks[i] = i
				}

				_, _, err = decodeSharedIDs(dir, noneComp(), n, blocks, entries, scatter)
				require.ErrorIs(t, err, ErrCorrupt)
			})
		}
	}
}

// FuzzSharedDictDecodeNoPanic feeds arbitrary bytes to the shared-dictionary bytes decode paths and
// reads every row of whatever they accept: a corrupt object must error, never panic — including at
// [chunk.DictColumn.At], where an out-of-range id only surfaces once a row is read.
func FuzzSharedDictDecodeNoPanic(f *testing.F) {
	f.Add(sharedIDObject(f, make([]int, 16), 4))
	f.Add([]byte{0})
	f.Add([]byte{2, 4, 1, 1, 0xff})

	desc := ColumnDesc{
		Name: "c", Kind: KindBytes, Blocked: true, Framed: true, Checked: true, SharedDict: true,
	}

	read := func(col *chunk.DictColumn, err error) {
		if err != nil || col == nil {
			return
		}

		for i := range col.Len() {
			_ = col.At(i)
		}
	}

	f.Fuzz(func(_ *testing.T, object []byte) {
		read(newColumnReader(desc, object, noneComp(), 16).Bytes())
		read(newColumnReader(desc, object, noneComp(), 16).DecodeBlocksBytes([]int{0, 1, 3}))
		read(newColumnReader(desc, object, noneComp(), 16).DecodeBlocksBytesIntoColumn([]int{0, 1, 3}))
	})
}

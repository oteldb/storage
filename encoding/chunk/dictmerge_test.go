package chunk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// dictOf builds a dictionary-encoded column from a value per row, so a test can hand [DictMerger]
// exactly the shape a decoded granule has.
func dictOf(vals ...string) *DictColumn {
	var (
		entries [][]byte
		index   = map[string]int{}
		ids     = make([]int, 0, len(vals))
	)

	for _, v := range vals {
		id, ok := index[v]
		if !ok {
			id = len(entries)
			index[v] = id
			entries = append(entries, []byte(v))
		}

		ids = append(ids, id)
	}

	return &DictColumn{Entries: entries, IDs: packIDs(ids, idWidthFor(len(entries))), IDWidth: idWidthFor(len(entries))}
}

func idWidthFor(entries int) int {
	if entries > 256 {
		return 2
	}

	return 1
}

func packIDs(ids []int, width int) []byte {
	out := make([]byte, len(ids)*width)
	for i, id := range ids {
		if width == 1 {
			out[i] = byte(id)

			continue
		}

		out[i*2], out[i*2+1] = byte(uint16(id)>>8), byte(uint16(id))
	}

	return out
}

// sharedIDsOf packs ids into a column dictionary of the given size, as a modeShared granule carries
// them.
func sharedIDsOf(entries int, ids ...int) ([]byte, int) {
	w := idWidthFor(entries)

	return packIDs(ids, w), w
}

func TestDictMergerSeedShared(t *testing.T) {
	t.Parallel()

	shared := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	var d DictMerger

	d.SeedShared(shared)

	ids, w := sharedIDsOf(len(shared), 2, 0, 1)
	d.AppendShared(ids, w, 3)

	ids2, _ := sharedIDsOf(len(shared), 1, 1)
	d.AppendShared(ids2, w, 2)

	got := d.Build()
	require.Positive(t, got.IDWidth, "a shared-only merge stays dictionary-encoded")
	require.Equal(t, 5, got.Len())

	for i, want := range []string{"c", "a", "b", "b", "b"} {
		require.Equalf(t, want, string(got.At(i)), "row %d", i)
	}

	require.Len(t, got.Entries, 3, "the seeded dictionary is the merged one")
}

// TestDictMergerSeedSharedIsLazyAboutOrder pins that seeding after a granule has already been merged
// keeps that granule's entries first, so the merged ids do not depend on when the seed happened.
func TestDictMergerSeedSharedIsLazyAboutOrder(t *testing.T) {
	t.Parallel()

	shared := [][]byte{[]byte("x"), []byte("y")}

	var d DictMerger

	d.Append(dictOf("self", "self")) // a self-encoded granule reaches the merge first
	d.SeedShared(shared)

	ids, w := sharedIDsOf(len(shared), 0, 1)
	d.AppendShared(ids, w, 2)

	got := d.Build()
	require.Equal(t, []string{"self", "x", "y"}, entryStrings(got.Entries))

	for i, want := range []string{"self", "self", "x", "y"} {
		require.Equalf(t, want, string(got.At(i)), "row %d", i)
	}
}

// TestDictMergerSeedSharedDedups pins that a self-encoded granule's values are merged against the
// seeded dictionary rather than appended blindly: a value in both places gets one entry.
func TestDictMergerSeedSharedDedups(t *testing.T) {
	t.Parallel()

	shared := [][]byte{[]byte("keep"), []byte("also")}

	var d DictMerger

	ids, w := sharedIDsOf(len(shared), 0, 1)

	d.SeedShared(shared)
	d.AppendShared(ids, w, 2)
	d.Append(dictOf("keep", "new", "keep")) // "keep" is already in the seeded dictionary

	got := d.Build()
	require.Equal(t, []string{"keep", "also", "new"}, entryStrings(got.Entries))

	for i, want := range []string{"keep", "also", "keep", "new", "keep"} {
		require.Equalf(t, want, string(got.At(i)), "row %d", i)
	}
}

// TestDictMergerUniqueGranule pins the asymmetry the seed introduces. Unseeded, a granule with one
// entry per row degrades the merge — there is nothing for its values to repeat into. Seeded, the other
// granules index a column-wide dictionary, and flattening for one unique granule would take their ids
// away too, so the dictionary is kept.
func TestDictMergerUniqueGranule(t *testing.T) {
	t.Parallel()

	unique := func() *DictColumn { return dictOf("u1", "u2", "u3") }

	t.Run("unseeded_degrades", func(t *testing.T) {
		t.Parallel()

		var d DictMerger

		d.Append(dictOf("r", "r", "r"))
		d.Append(unique())

		got := d.Build()
		require.Zero(t, got.IDWidth, "no seeded dictionary to repeat into: flatten")
		require.Equal(t, []string{"r", "r", "r", "u1", "u2", "u3"}, rowStrings(got))
	})

	t.Run("seeded_keeps_dictionary", func(t *testing.T) {
		t.Parallel()

		shared := [][]byte{[]byte("r")}

		var d DictMerger

		ids, w := sharedIDsOf(len(shared), 0, 0, 0)

		d.SeedShared(shared)
		d.AppendShared(ids, w, 3)
		d.Append(unique())

		got := d.Build()
		require.Positive(t, got.IDWidth, "the shared granules keep their ids")
		require.Equal(t, []string{"r", "u1", "u2", "u3"}, entryStrings(got.Entries))
		require.Equal(t, []string{"r", "r", "r", "u1", "u2", "u3"}, rowStrings(got))
	})
}

// TestDictMergerCeilingDegrades pins that a granule which cannot fit the remaining dictionary room
// degrades the merge, values intact — and that the decision is taken for the whole granule rather than
// part-way through its entries.
func TestDictMergerCeilingDegrades(t *testing.T) {
	t.Parallel()

	shared := make([][]byte, maxDictEntries)
	for i := range shared {
		shared[i] = fmt.Appendf(nil, "s%d", i)
	}

	var d DictMerger

	ids, w := sharedIDsOf(len(shared), 0, 1)

	d.SeedShared(shared)
	d.AppendShared(ids, w, 2)
	d.Append(dictOf("over", "flow"))

	got := d.Build()
	require.Zero(t, got.IDWidth, "a full dictionary cannot take another entry: flatten")
	require.Equal(t, []string{"s0", "s1", "over", "flow"}, rowStrings(got))
}

func TestDictMergerSeedSharedScatter(t *testing.T) {
	t.Parallel()

	shared := [][]byte{[]byte("a"), []byte("b")}

	var d DictMerger

	d.Scatter(6)
	d.SeedShared(shared)

	ids, w := sharedIDsOf(len(shared), 1, 0)
	d.AppendSharedAt(ids, w, 2, 4) // the last two rows of a six-row column

	got := d.Build()
	require.Equal(t, 6, got.Len())
	require.Equal(t, "b", string(got.At(4)))
	require.Equal(t, "a", string(got.At(5)))
}

func entryStrings(entries [][]byte) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = string(e)
	}

	return out
}

func rowStrings(c *DictColumn) []string {
	out := make([]string, c.Len())
	for i := range c.Len() {
		out[i] = string(c.At(i))
	}

	return out
}

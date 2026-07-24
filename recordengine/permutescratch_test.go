package recordengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortByTsWith must produce the same ordering whether it permutes through a shared scratch column or
// allocates a fresh destination per column — the scratch is a reuse optimization, not a behavior
// change.
func TestSortByTsWithScratchMatchesAllocating(t *testing.T) {
	t.Parallel()

	build := func() *recordCols {
		s := NewSchema(
			Column{Name: "a", Kind: KindBytes},
			Column{Name: "b", Kind: KindBytes},
		)
		c := newRecordCols(s, 0, fullSel(s))

		// Deliberately out of ts order, with differing-width byte columns.
		ts := []int64{50, 10, 30, 10, 20}
		for i, t := range ts {
			c.ts = append(c.ts, t)
			c.bytes[0].appendCell(fmt.Appendf(nil, "a%d", i))
			c.bytes[1].appendCell(fmt.Appendf(nil, "bbb%d", i))
		}

		return c
	}

	alloc := build()
	alloc.sortByTsWith(nil)

	var scratch byteCol
	shared := build()
	shared.sortByTsWith(&scratch)

	require.Equal(t, alloc.ts, shared.ts, "same ts order")

	for k := range alloc.bytes {
		want := make([]string, alloc.bytes[k].rows())
		got := make([]string, shared.bytes[k].rows())
		for i := range want {
			want[i] = string(alloc.bytes[k].at(i))
			got[i] = string(shared.bytes[k].at(i))
		}
		assert.Equalf(t, want, got, "column %d matches", k)
	}

	// The scratch ends holding one of the columns' old backing arrays (from the final swap); it is
	// reusable, which is the whole point — a second sort must not corrupt an already-sorted buffer.
	shared.sortByTsWith(&scratch)
	assert.True(t, shared.isSortedByTs(), "a second sort of an ordered buffer is a no-op")
}

func TestPermuteBytesIntoReusesArrays(t *testing.T) {
	t.Parallel()

	src := byteCol{}
	for i := range 8 {
		src.appendCell(fmt.Appendf(nil, "v%d", i))
	}

	idx := []int{7, 6, 5, 4, 3, 2, 1, 0}

	var dst byteCol
	permuteBytesInto(&dst, &src, idx)
	first := &dst.data[0]

	// A second permute into the same dst must reuse the backing array (no realloc) when capacity fits.
	permuteBytesInto(&dst, &src, idx)
	assert.Same(t, first, &dst.data[0], "dst.data is reused across permutes")

	for p, j := range idx {
		assert.Equalf(t, fmt.Sprintf("v%d", j), string(dst.at(p)), "row %d", p)
	}
}

// BenchmarkSortByTsScratch measures the flush-sort permute path: many streams whose records are out
// of ts order, so every byte column is reordered. The shared scratch reuses one destination across
// all of them; the nil path allocates one per byte column per stream.
func BenchmarkSortByTsScratch(b *testing.B) {
	const (
		streams = 64
		rows    = 512
	)
	schema := NewSchema(
		Column{Name: "body", Kind: KindBytes},
		Column{Name: "attrs", Kind: KindBytes},
	)

	build := func() []*recordCols {
		cs := make([]*recordCols, streams)
		for s := range streams {
			c := newRecordCols(schema, rows, fullSel(schema))
			for i := range rows {
				c.ts = append(c.ts, int64(rows-i)) // strictly descending ⇒ always needs sorting
				c.bytes[0].appendCell([]byte("some log body text here"))
				c.bytes[1].appendCell([]byte("k1=v1 k2=v2"))
			}
			cs[s] = c
		}

		return cs
	}

	b.Run("scratch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var scratch byteCol
			for _, c := range build() {
				c.sortByTsWith(&scratch)
			}
		}
	})

	b.Run("alloc", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, c := range build() {
				c.sortByTsWith(nil)
			}
		}
	})
}

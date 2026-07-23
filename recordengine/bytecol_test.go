package recordengine

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cells reads back a column's cells as strings — not [][]byte, because an empty cell is nil in one
// form and zero-length in the other, which no reader can observe.
func cells(b *byteCol) []string {
	out := make([]string, 0, b.rows())
	for i := range b.rows() {
		out = append(out, string(b.at(i)))
	}

	return out
}

func logicalOf(vals []string) int64 {
	var n int64
	for _, v := range vals {
		n += int64(len(v))
	}

	return n
}

func fill(cs ...string) *byteCol {
	var b byteCol
	for _, c := range cs {
		b.appendCell([]byte(c))
	}

	return &b
}

// repeat appends n cells drawn cyclically from vals, so the column stays interned.
func repeat(n int, vals ...string) *byteCol {
	var b byteCol
	for i := range n {
		b.appendCell([]byte(vals[i%len(vals)]))
	}

	return &b
}

func TestByteColReadBack(t *testing.T) {
	t.Parallel()

	for _, tt := range [][]string{
		{},
		{"a"},
		{"a", "a", "a"},
		{"", "", ""},
		{"a", "b"},
		{"a", "a", "a", "b"},
		{"ab", "a"},
		{"a", "", "b", ""},
	} {
		t.Run(fmt.Sprint(tt), func(t *testing.T) {
			t.Parallel()

			b := fill(tt...)
			require.Equal(t, len(tt), b.rows())
			assert.Equal(t, tt, cells(b))
			assert.Equal(t, logicalOf(tt), b.byteSize())
		})
	}
}

// TestByteColInternsRepeats is the point of the form: a column of few distinct values stores each
// once plus a 4-byte id per row, while still reporting the logical size to the accounting path.
func TestByteColInternsRepeats(t *testing.T) {
	t.Parallel()

	const (
		rows  = 10000
		width = 300
	)

	x, y := bytes.Repeat([]byte("x"), width), bytes.Repeat([]byte("y"), width)
	b := repeat(rows, string(x), string(y))

	require.True(t, b.interned, "stays interned")
	assert.Equal(t, rows, b.rows())
	assert.Equal(t, int64(2*width), int64(len(b.data)), "one copy per distinct value")
	assert.Equal(t, int64(2*width+4*rows), b.storedSize())
	assert.Equal(t, int64(rows*width), b.byteSize(), "accounting still sees the logical bytes")
}

// TestByteColBailsOnUniqueValues is the other half: a near-unique column (a span id, a trace id, a
// verbose body) must not pay a hash per row for a dictionary that saves nothing.
func TestByteColBailsOnUniqueValues(t *testing.T) {
	t.Parallel()

	var b byteCol
	for i := range internCheck * 4 {
		b.appendCell(fmt.Append(nil, "span-", i))
	}

	require.False(t, b.interned, "unique values bail out of the interned form")
	assert.True(t, b.noIntern, "and never intern again")
	assert.Equal(t, internCheck*4, b.rows())
	assert.Equal(t, "span-7", string(b.at(7)))
	assert.Equal(t, b.byteSize(), b.storedSize(), "a flat column stores exactly its logical bytes")
}

// TestByteColBailsWhenValuesDiverge covers a column that starts repetitive and turns unique — the
// reason the threshold is re-tested rather than decided once.
func TestByteColBailsWhenValuesDiverge(t *testing.T) {
	t.Parallel()

	b := repeat(internCheck, "same")
	require.True(t, b.interned, "a repetitive prefix interns")

	want := cells(b)

	for i := range internCheck * 8 {
		cell := fmt.Append(nil, "unique-value-", i)
		b.appendCell(cell)
		want = append(want, string(cell))
	}

	assert.False(t, b.interned, "and bails once the values stop repeating")
	assert.Equal(t, want, cells(b), "every cell survives the transition")
	assert.Equal(t, logicalOf(want), b.byteSize())
}

// TestByteColExpandPreservesContents: expanding changes the representation, not the contents.
func TestByteColExpandPreservesContents(t *testing.T) {
	t.Parallel()

	b := repeat(1000, "a", "b", "c")
	require.True(t, b.interned)

	want, size := cells(b), b.byteSize()

	b.expand()
	assert.False(t, b.interned)
	assert.Equal(t, want, cells(b))
	assert.Equal(t, size, b.byteSize(), "the logical size is form-independent")
}

func TestByteColEnsureClearsInterning(t *testing.T) {
	t.Parallel()

	b := repeat(1000, "a")
	require.True(t, b.interned)

	b.ensure(4)
	assert.Zero(t, b.rows(), "ensure re-arms the column")
	assert.Zero(t, b.byteSize())

	b.appendCell([]byte("z"))
	assert.Equal(t, []string{"z"}, cells(b))
}

func TestByteColKeepAndGatherInterned(t *testing.T) {
	t.Parallel()

	b := repeat(1000, "aa", "b")
	b.keep(2, 6)
	assert.Equal(t, []string{"aa", "b", "aa", "b"}, cells(b))
	assert.Equal(t, int64(6), b.byteSize(), "the logical size follows the kept rows")

	b2 := repeat(1000, "aa", "b")
	b2.gather([]int{0, 3, 4})
	assert.Equal(t, []string{"aa", "b", "aa"}, cells(b2))
	assert.Equal(t, int64(5), b2.byteSize())
}

func TestByteColAppendRangeAcrossForms(t *testing.T) {
	t.Parallel()

	flat := func(cs ...string) *byteCol {
		b := fill(cs...)
		b.expand()

		return b
	}

	for _, tt := range []struct {
		name     string
		dst, src *byteCol
		want     []string
	}{
		{"interned into empty", &byteCol{}, repeat(3, "v"), []string{"v", "v", "v"}},
		{"flat into empty", &byteCol{}, flat("a", "b"), []string{"a", "b"}},
		{"flat into interned", repeat(2, "v"), flat("a", "b"), []string{"v", "v", "a", "b"}},
		{"interned into flat", flat("a", "b"), repeat(2, "v"), []string{"a", "b", "v", "v"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.dst.appendRange(tt.src, 0, tt.src.rows())
			assert.Equal(t, tt.want, cells(tt.dst))
			assert.Equal(t, logicalOf(tt.want), tt.dst.byteSize())
		})
	}
}

func TestByteColPermuteInterned(t *testing.T) {
	t.Parallel()

	src := repeat(6, "aa", "b", "ccc")
	idx := []int{5, 0, 3, 1, 4, 2}

	var dst byteCol
	permuteBytesInto(&dst, src, idx)
	require.True(t, dst.interned, "an interned source permutes its id index, sharing the dictionary")

	want := make([]string, 0, len(idx))
	for _, j := range idx {
		want = append(want, string(src.at(j)))
	}

	assert.Equal(t, want, cells(&dst))
	assert.Equal(t, logicalOf(want), dst.byteSize())

	src.expand()
	general := permuteBytes(src, idx)
	assert.Equal(t, cells(&general), cells(&dst), "both forms permute alike")
}

func TestByteColViewsInterned(t *testing.T) {
	t.Parallel()

	b := repeat(4, "v", "w")
	assert.Equal(t, [][]byte{[]byte("v"), []byte("w"), []byte("v"), []byte("w")}, b.views(nil))
}

// FuzzByteColForms: a column reads back exactly the cells appended, wherever it is expanded.
func FuzzByteColForms(f *testing.F) {
	f.Add([]byte("a\x00a\x00a"), 2)
	f.Add([]byte("a\x00b"), 0)
	f.Add([]byte(""), 1)

	f.Fuzz(func(t *testing.T, raw []byte, cut int) {
		parts := bytes.Split(raw, []byte{0})

		want := make([]string, 0, len(parts))
		for _, p := range parts {
			want = append(want, string(p))
		}

		var b byteCol
		for _, p := range parts {
			b.appendCell(p)
		}

		require.Equal(t, len(parts), b.rows())
		require.Equal(t, want, cells(&b))
		require.Equal(t, logicalOf(want), b.byteSize())

		// Expanding mid-stream must not change what a reader sees.
		var c byteCol
		for i, p := range parts {
			if cut >= 0 && i == cut%max(len(parts), 1) {
				c.expand()
			}

			c.appendCell(p)
		}

		c.expand()
		require.Equal(t, cells(&b), cells(&c))
		require.Equal(t, b.byteSize(), c.byteSize())
	})
}

func BenchmarkByteColAppend(b *testing.B) {
	for _, width := range []int{16, 300} {
		cell := bytes.Repeat([]byte("x"), width)

		b.Run(fmt.Sprintf("repeated/%dB", width), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(width))

			var col byteCol
			for b.Loop() {
				if col.rows() >= 4096 {
					col = byteCol{}
				}

				col.appendCell(cell)
			}
		})

		b.Run(fmt.Sprintf("unique/%dB", width), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(width))

			vals := make([][]byte, 4096)
			for i := range vals {
				vals[i] = slices.Concat(fmt.Append(nil, i), cell)
			}

			var col byteCol

			i := 0
			for b.Loop() {
				if col.rows() >= 4096 {
					col = byteCol{}
				}

				col.appendCell(vals[i%len(vals)])
				i++
			}
		})
	}
}

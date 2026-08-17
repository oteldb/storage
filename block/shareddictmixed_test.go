package block

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
)

// mixedSharedValues builds a column whose granules repeat enough to join a shared dictionary, except
// the ones named in self, which are near-unique and so encode themselves. That mix is the shape a
// real column takes when a service logs structured lines and then dumps something unique — and the
// shape whose whole-column decode used to fall off a cliff, because one self-encoded granule sent
// every *other* granule through the merge carrying the column dictionary as its own.
func mixedSharedValues(granules, rows int, self map[int]bool, selfDistinct int) [][]byte {
	vals := make([][]byte, 0, granules*rows)

	for g := range granules {
		for r := range rows {
			if self[g] {
				// selfDistinct == rows makes the granule fully unique; fewer gives it internal repeats
				// while still declining the shared dictionary (which needs distinct*2 <= rows).
				vals = append(vals, fmt.Appendf(nil, "unique-%d-%d-payload", g, r%selfDistinct))

				continue
			}

			// Two rows per distinct value: exactly at sharedDictMinRepeat, and each granule draws from
			// an overlapping window of a column-wide population, so the shared dictionary is large.
			vals = append(vals, fmt.Appendf(nil, "shared-%d", g*(rows/4)+r/2))
		}
	}

	return vals
}

func TestSharedDictMixedDecodeMatchesValues(t *testing.T) {
	t.Parallel()

	const (
		granules = 6
		rows     = 512
	)

	for _, tc := range []struct {
		name string
		self map[int]bool
	}{
		{"all_shared", nil},
		{"first_self", map[int]bool{0: true}},
		{"middle_self", map[int]bool{2: true}},
		{"last_self", map[int]bool{granules - 1: true}},
		{"alternating", map[int]bool{1: true, 3: true, 5: true}},
		{"all_self", map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: true}},
	} {
		for _, sd := range []struct {
			name     string
			distinct int
		}{
			{"unique_self", rows},            // degrades the merge to the flat form
			{"repeating_self", 3 * rows / 5}, // declines the shared dictionary, keeps its own
		} {
			t.Run(tc.name+"/"+sd.name, func(t *testing.T) {
				t.Parallel()

				runMixedValues(t, granules, rows, tc.self, sd.distinct)
			})
		}
	}
}

func runMixedValues(t *testing.T, granules, rows int, self map[int]bool, selfDistinct int) {
	t.Helper()

	vals := mixedSharedValues(granules, rows, self, selfDistinct)

	desc, obj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		zstdComp(), rows, defaultCompressBlockBytes)
	require.NoError(t, err)

	r := newColumnReader(desc, obj, zstdComp(), len(vals))

	whole, err := r.Bytes()
	require.NoError(t, err)
	require.Equal(t, len(vals), whole.Len(), "row count")

	for i, want := range vals {
		require.Equalf(t, want, whole.At(i), "row %d of the whole-column decode", i)
	}

	// Every single-granule decode, and one multi-granule window per start, so a selection that
	// mixes the modes is covered in both directions.
	if !desc.Blocked {
		return // no granule joined, so the writer emitted a single unframed stream
	}

	for g := range granules {
		sub := newColumnReader(desc, obj, zstdComp(), len(vals))

		got, err := sub.DecodeBlocksBytes([]int{g})
		require.NoErrorf(t, err, "granule %d", g)
		require.Equalf(t, rows, got.Len(), "granule %d row count", g)

		for i := range rows {
			require.Equalf(t, vals[g*rows+i], got.At(i), "granule %d row %d", g, i)
		}
	}

	for g := range granules - 1 {
		sub := newColumnReader(desc, obj, zstdComp(), len(vals))

		got, err := sub.DecodeBlocksBytesIntoColumn([]int{g, g + 1})
		require.NoErrorf(t, err, "granules %d,%d", g, g+1)

		// Scatter mode keeps part row indices: the selected rows sit at their own offsets.
		for i := g * rows; i < (g+2)*rows; i++ {
			require.Equalf(t, vals[i], got.At(i), "granules %d,%d row %d", g, g+1, i)
		}
	}
}

// TestSharedDictMixedDecodeDictionaryShape pins that a mixed decode still returns a *dictionary*
// rather than degrading to the flat form: the per-distinct-entry predicate memo is the reason the
// encoding exists, and a merge that flattened would hand it back.
func TestSharedDictMixedDecodeDictionaryShape(t *testing.T) {
	t.Parallel()

	const (
		granules = 6
		rows     = 512
	)

	// A self-encoded granule that still repeats internally keeps the merge dictionary-encoded, and the
	// seeded shared entries are deduped against its own rather than appended blindly.
	vals := mixedSharedValues(granules, rows, map[int]bool{2: true}, 3*rows/5)

	desc, obj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
		zstdComp(), rows, defaultCompressBlockBytes)
	require.NoError(t, err)
	require.True(t, desc.SharedDict, "the repeating granules must have joined a shared dictionary")

	whole, err := newColumnReader(desc, obj, zstdComp(), len(vals)).Bytes()
	require.NoError(t, err)
	require.Positive(t, whole.IDWidth, "a mixed decode must stay dictionary-encoded")

	distinct := map[string]struct{}{}
	for _, v := range vals {
		distinct[string(v)] = struct{}{}
	}

	require.Len(t, whole.Entries, len(distinct), "merged dictionary must hold each distinct value once")

	// A *fully unique* self-encoded granule still flattens the whole decode: [chunk.DictMerger.Append]
	// degrades when a source granule holds one entry per row. That predates this seam and is left
	// alone here — it costs a mixed column its predicate memo, which is worth its own change.
	uniq := mixedSharedValues(granules, rows, map[int]bool{2: true}, rows)

	uDesc, uObj, err := buildColumn(
		Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: uniq, Block: true},
		zstdComp(), rows, defaultCompressBlockBytes)
	require.NoError(t, err)

	flat, err := newColumnReader(uDesc, uObj, zstdComp(), len(uniq)).Bytes()
	require.NoError(t, err)
	require.Zero(t, flat.IDWidth, "a fully unique self-encoded granule degrades the merge to flat")
}

// BenchmarkSharedDictMixedDecode measures the whole-column decode of a shared-dictionary column with
// and without a self-encoded granule in it. The two used to differ by 4-13x on real columns (#358):
// one self-encoded granule made the decode take the merge path for *every* granule, and each shared
// granule then arrived carrying the whole column dictionary, costing a hash probe per entry per
// granule.
func BenchmarkSharedDictMixedDecode(b *testing.B) {
	const (
		granules = 40
		rows     = 1024
	)

	for _, tc := range []struct {
		name     string
		self     map[int]bool
		distinct int
	}{
		{"all_shared", nil, rows},
		{"one_self_repeating", map[int]bool{granules / 2: true}, 3 * rows / 5},
		{"one_self_unique", map[int]bool{granules / 2: true}, rows},
		{"five_self_repeating", map[int]bool{3: true, 11: true, 19: true, 27: true, 35: true}, 3 * rows / 5},
	} {
		vals := mixedSharedValues(granules, rows, tc.self, tc.distinct)

		var logical int64
		for _, v := range vals {
			logical += int64(len(v))
		}

		desc, obj, err := buildColumn(
			Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
			zstdComp(), rows, defaultCompressBlockBytes)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(logical)

			for b.Loop() {
				r := newColumnReader(desc, obj, zstdComp(), len(vals))
				if _, err := r.Bytes(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// FuzzSharedDictWindowedMixed fuzzes a windowed decode of a column that mixes granule modes: the
// selection may hold shared granules, self-encoded ones or both, and every selected row must decode
// to its value at its own part row index (scatter mode's contract).
func FuzzSharedDictWindowedMixed(f *testing.F) {
	f.Add(2000, 40, 128, uint64(1))
	f.Add(300, 300, 16, uint64(7))
	f.Add(5000, 700, 512, uint64(42))

	f.Fuzz(func(t *testing.T, rows, distinct, granule int, pick uint64) {
		if rows <= 0 || rows > 20000 || distinct <= 0 || granule <= 0 || granule > 8192 {
			t.Skip()
		}

		vals := make([][]byte, rows)
		for i := range rows {
			// Whole granules of unique values, so some decline the shared dictionary while their
			// neighbors join it.
			if (i/granule)%3 == 0 {
				vals[i] = fmt.Appendf(nil, "u-%d", i)
			} else {
				vals[i] = fmt.Appendf(nil, "v-%d", i%distinct)
			}
		}

		desc, obj, err := buildColumn(
			Column{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Bytes: vals, Block: true},
			noneComp(), granule, defaultCompressBlockBytes)
		if err != nil {
			t.Fatal(err)
		}

		if !desc.Blocked {
			t.Skip() // no granule joined: the writer emitted one unframed stream, nothing to select
		}

		granules := (rows + granule - 1) / granule

		var blocks []int
		for g := range granules {
			if pick>>(g%64)&1 == 1 {
				blocks = append(blocks, g)
			}
		}

		if len(blocks) == 0 {
			t.Skip()
		}

		got, err := newColumnReader(desc, obj, noneComp(), rows).DecodeBlocksBytesIntoColumn(blocks)
		if err != nil {
			t.Fatal(err)
		}

		for _, g := range blocks {
			for i := g * granule; i < min((g+1)*granule, rows); i++ {
				if !bytes.Equal(got.At(i), vals[i]) {
					t.Fatalf("granule %d row %d: got %q, want %q", g, i, got.At(i), vals[i])
				}
			}
		}
	})
}

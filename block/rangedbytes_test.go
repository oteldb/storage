package block

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// writeBytesPart writes vals as a single block-framed bytes column and returns a reader over it,
// together with the counting backend it landed on.
func writeBytesPart(t *testing.T, vals [][]byte, granule int, opts ...PartOption) (*PartReader, backendtest.SizedByteCounter) {
	t.Helper()

	ctx := context.Background()
	b := backendtest.NewSizedByteCounter(backendtest.NewStreamingMemory())

	opts = append([]PartOption{
		WithGranuleSize(granule),
		WithCompression(compress.AlgorithmZSTD),
		WithCompressBlockBytes(1024),
	}, opts...)

	w := NewPartWriter(opts...)
	require.NoError(t, w.AddColumn(Column{
		Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Block: true, Bytes: vals,
	}))
	require.NoError(t, WritePart(ctx, b, "p", w))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	return r, b
}

// numericRows is a three-column metric corpus with enough rows to fill several granules.
func numericRows(n int) metricRows {
	var m metricRows

	for i := range n {
		m.series = append(m.series, chunk.U128{Lo: uint64(i / 512)})
		m.ts = append(m.ts, int64(i))
		m.value = append(m.value, float64(i))
	}

	return m
}

// sharedDictCases are the granule mode patterns a bytes column can take: all granules on the shared
// dictionary, none, and every mixture in between. The self-encoded ones are what a ranged read has
// to get right — their values live inside frames, not in the column's leading dictionary.
func sharedDictCases(granules int) []struct {
	name string
	self map[int]bool
} {
	all := map[int]bool{}
	for g := range granules {
		all[g] = true
	}

	return []struct {
		name string
		self map[int]bool
	}{
		{"all_shared", nil},
		{"first_self", map[int]bool{0: true}},
		{"middle_self", map[int]bool{granules / 2: true}},
		{"last_self", map[int]bool{granules - 1: true}},
		{"alternating", map[int]bool{1: true, 3: true, 5: true}},
		{"all_self", all},
	}
}

// TestColumnBlocksMatchesWholeBytesColumn is the correctness bar this fixes: the ranged reader must
// agree with the whole-object one on a bytes column, whole and per granule. Before the shared
// dictionary was peeled, opening such a column parsed its dictionary bytes as a block directory and
// failed as corruption — on healthy data, which on a load path is fatal rather than degrading.
func TestColumnBlocksMatchesWholeBytesColumn(t *testing.T) {
	t.Parallel()

	const (
		granules = 6
		rows     = 512
	)

	for _, tc := range sharedDictCases(granules) {
		for _, sd := range []struct {
			name     string
			distinct int
		}{
			{"unique_self", rows},
			{"repeating_self", 3 * rows / 5},
		} {
			t.Run(tc.name+"/"+sd.name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				vals := mixedSharedValues(granules, rows, tc.self, sd.distinct)

				r, _ := writeBytesPart(t, vals, rows)

				desc, ok := r.ColumnDescByName("attrs")
				require.True(t, ok)

				if !desc.Blocked {
					// No granule joined, so the writer emitted one unframed stream: there is no
					// directory to range over and Column is the only reader.
					_, err := r.ColumnBlocks(ctx, "attrs")
					require.Error(t, err)

					return
				}

				d, err := r.ColumnBlocks(ctx, "attrs")
				require.NoError(t, err)
				require.Equal(t, granules, d.NumBlocks())

				whole, err := d.DecodeBytes(nil)
				require.NoError(t, err)
				require.Equal(t, len(vals), whole.Len())

				for i, want := range vals {
					require.Equalf(t, want, whole.At(i), "row %d of the ranged whole-column decode", i)
				}

				for g := range granules {
					sub, err := d.DecodeBytes([]int{g})
					require.NoErrorf(t, err, "granule %d", g)
					require.Equalf(t, rows, sub.Len(), "granule %d row count", g)

					for i := range rows {
						require.Equalf(t, vals[g*rows+i], sub.At(i), "granule %d row %d", g, i)
					}
				}
			})
		}
	}
}

// TestColumnBlocksSharedEntriesMatchesWholeColumn pins the dictionary the ranged open peels against
// the one the whole-object reader parses, entry for entry and in order: the ids in every granule
// index it positionally, so an entry set that merely agrees as a set would still decode wrong.
func TestColumnBlocksSharedEntriesMatchesWholeColumn(t *testing.T) {
	t.Parallel()

	const (
		granules = 6
		rows     = 512
	)

	ctx := context.Background()
	vals := mixedSharedValues(granules, rows, map[int]bool{2: true}, rows)

	r, _ := writeBytesPart(t, vals, rows)

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.SharedDict)

	col, err := r.Column(ctx, "attrs")
	require.NoError(t, err)

	want, err := col.sharedEntries()
	require.NoError(t, err)
	require.True(t, want.on)

	wantEntries := want.entries
	require.NotEmpty(t, wantEntries)

	d, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)
	require.Equal(t, wantEntries, d.SharedEntries())
}

// TestColumnBlocksBytesReadsOnlyWhatItDecodes is the point of ranging a bytes column: a granule's
// decode must not pull the whole object. The shared dictionary is the fixed cost — it is read at
// open because every granule resolves ids against it — so the bar is the dictionary plus the
// granule, not the column.
func TestColumnBlocksBytesReadsOnlyWhatItDecodes(t *testing.T) {
	t.Parallel()

	const (
		granules = 16
		rows     = 512
	)

	ctx := context.Background()
	vals := mixedSharedValues(granules, rows, nil, rows)

	r, b := writeBytesPart(t, vals, rows)

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.Blocked)

	b.Reset()

	d, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)

	_, err = d.DecodeBytes([]int{0})
	require.NoError(t, err)

	assert.Less(t, b.Bytes(), desc.Bytes,
		"a one-granule ranged decode read the whole %d-byte column", desc.Bytes)
}

// TestColumnBlocksRejectsBytesDecodeOfNumericColumn: a numeric column's granule stream has no mode
// byte and no dictionary, so decoding it as bytes would not fail — it would return values.
func TestColumnBlocksRejectsBytesDecodeOfNumericColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	b := writeRangedPart(t, numericRows(4096), false,
		WithSortKey("ts"), WithGranuleSize(256),
		WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	d, err := r.ColumnBlocks(ctx, "ts")
	require.NoError(t, err)

	_, err = d.DecodeBytes(nil)
	require.Error(t, err)
	assert.Nil(t, d.SharedEntries())
}

// offsetBackend serves one key's object shifted right by pad bytes, so a reader that must locate a
// block-framed container somewhere other than the object's head can be exercised against a real
// object of either layout. A shared-dictionary column is exactly that shape, and its footer form is
// not yet writable — the streaming writer does not take bytes columns — so the offset is injected
// rather than waited for.
type offsetBackend struct {
	backend.Backend

	key string
	pad int64
}

func (b *offsetBackend) Read(ctx context.Context, key string) ([]byte, error) {
	v, err := b.Backend.Read(ctx, key)
	if err != nil || key != b.key {
		return v, err
	}

	return append(make([]byte, b.pad), v...), nil
}

func (b *offsetBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if key != b.key {
		return backend.ReadAt(ctx, b.Backend, key, off, n)
	}

	v, err := b.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	if off > int64(len(v)) {
		return nil, nil
	}

	return v[off:min(off+n, int64(len(v)))], nil
}

func (b *offsetBackend) Size(ctx context.Context, key string) (int64, error) {
	n, err := backend.SizeOf(ctx, b.Backend, key)
	if err != nil || key != b.key {
		return n, err
	}

	return n + b.pad, nil
}

// TestBlockDirReadsContainerAtOffset covers the offset arithmetic both directory readers now carry.
// A leading directory's frames sit after it and a footer directory's before it, so a container that
// does not start at the object's head shifts every frame under one layout and the directory itself
// under the other — and the footer reader used to hard-code the frames to offset 0, which a
// dictionary ahead of them makes wrong.
func TestBlockDirReadsContainerAtOffset(t *testing.T) {
	t.Parallel()

	for _, streamed := range []bool{false, true} {
		t.Run(layoutName(streamed), func(t *testing.T) {
			t.Parallel()

			const pad = 37

			ctx := context.Background()

			rows := numericRows(4096)

			inner := writeRangedPart(t, rows, streamed,
				WithSortKey("ts"), WithGranuleSize(256),
				WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

			r, err := OpenPart(ctx, inner, "p")
			require.NoError(t, err)

			i, ok := r.byName["ts"]
			require.True(t, ok)

			desc := r.manifest.Columns[i]
			require.True(t, desc.Blocked)
			require.Equal(t, streamed, desc.Footer)

			key := columnKey("p", i)
			shifted := &offsetBackend{Backend: inner, key: key, pad: pad}

			read := readLeadingDir
			if desc.Footer {
				read = readFooterDir
			}

			want, err := read(ctx, inner, key, 0, desc.Bytes, dirCheck{checked: desc.Checked, rawMax: unlimited})
			require.NoError(t, err)

			got, err := read(ctx, shifted, key, pad, desc.Bytes, dirCheck{checked: desc.Checked, rawMax: unlimited})
			require.NoError(t, err)
			require.Equal(t, want.nBlocks(), got.nBlocks())
			require.Equal(t, want.dataOff+pad, got.dataOff)

			want.col, got.col = desc.Name, desc.Name
			want.src = &frameSource{ctx: ctx, b: inner, key: key, base: want.dataOff}
			got.src = &frameSource{ctx: ctx, b: shifted, key: key, base: got.dataOff}

			for g := range want.nBlocks() {
				a := newBlockStreams(want, zstdComp())
				c := newBlockStreams(got, zstdComp())

				sa, err := a.granule(g)
				require.NoErrorf(t, err, "granule %d at offset 0", g)

				sc, err := c.granule(g)
				require.NoErrorf(t, err, "granule %d at offset %d", g, pad)

				require.Equalf(t, sa, sc, "granule %d", g)
			}
		})
	}
}

// FuzzColumnBlocksBytes drives the granule mode pattern, granule size and value shape from the
// fuzzer, asserting the ranged reader and the whole-object one agree row for row.
func FuzzColumnBlocksBytes(f *testing.F) {
	f.Add(uint32(0b000000), 64, 4, 8)
	f.Add(uint32(0b101010), 128, 6, 3)
	f.Add(uint32(0b111111), 96, 5, 96)
	f.Add(uint32(0b000001), 256, 3, 256)

	f.Fuzz(func(t *testing.T, modes uint32, rows, granules, distinct int) {
		if rows < 2 || rows > 512 || granules < 1 || granules > 12 || distinct < 1 || distinct > rows {
			t.Skip()
		}

		self := map[int]bool{}
		for g := range granules {
			if modes&(1<<uint(g%32)) != 0 {
				self[g] = true
			}
		}

		ctx := context.Background()
		vals := mixedSharedValues(granules, rows, self, distinct)

		r, _ := writeBytesPart(t, vals, rows)

		desc, ok := r.ColumnDescByName("attrs")
		if !ok || !desc.Blocked {
			t.Skip()
		}

		col, err := r.Column(ctx, "attrs")
		if err != nil {
			t.Fatal(err)
		}

		whole, err := col.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		d, err := r.ColumnBlocks(ctx, "attrs")
		if err != nil {
			t.Fatal(err)
		}

		ranged, err := d.DecodeBytes(nil)
		if err != nil {
			t.Fatal(err)
		}

		if whole.Len() != ranged.Len() {
			t.Fatalf("ranged decoded %d rows, whole %d", ranged.Len(), whole.Len())
		}

		for i := range whole.Len() {
			if !bytes.Equal(whole.At(i), ranged.At(i)) {
				t.Fatalf("row %d: ranged %q, whole %q", i, ranged.At(i), whole.At(i))
			}
		}
	})
}

// TestColumnBlocksReadsDictionaryLargerThanProbe covers the case real data is in: a record
// attributes column's dictionary runs to tens of kilobytes, well past the header probe, so the
// extent taken from its two leading uvarints has to be right or the peel lands mid-dictionary.
func TestColumnBlocksReadsDictionaryLargerThanProbe(t *testing.T) {
	t.Parallel()

	const (
		granules = 16
		rows     = 2048
	)

	ctx := context.Background()

	vals := make([][]byte, 0, granules*rows)
	for g := range granules {
		for r := range rows {
			vals = append(vals, fmt.Appendf(nil, "shared-%08d-%s", g*(rows/2)+r/2, "padding-to-defeat-compression"))
		}
	}

	r, _ := writeLeadingBytesPart(t, vals, rows)

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.SharedDict)

	col, err := r.Column(ctx, "attrs")
	require.NoError(t, err)

	want, err := col.sharedEntries()
	require.NoError(t, err)
	require.True(t, want.on)

	wantEntries := want.entries

	_, off, err := readSharedDict(ctx, r.b, columnKey("p", r.byName["attrs"]), zstdComp(), desc.Bytes, desc.Checked, unlimited)
	require.NoError(t, err)
	require.Greater(t, off, int64(dirProbeBytes), "the dictionary fits the probe, so this covers nothing")

	d, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)
	require.Equal(t, wantEntries, d.SharedEntries())

	got, err := d.DecodeBytes(nil)
	require.NoError(t, err)
	require.Equal(t, len(vals), got.Len())

	for i, v := range vals {
		require.Equalf(t, v, got.At(i), "row %d", i)
	}
}

// TestSharedDictHeadLen pins the header extent arithmetic against the shapes a damaged object takes.
// It is the bound every later read of the dictionary is sized by, so a wrong answer here reads
// either too little (a parse that fails somewhere else) or past the object.
func TestSharedDictHeadLen(t *testing.T) {
	t.Parallel()

	// [uvarint entryCount][uvarint compressedDictLen][compressed dict][u32le CRC32C]
	head := func(count, packedLen uint64, packed int) []byte {
		b := binary.AppendUvarint(nil, count)
		b = binary.AppendUvarint(b, packedLen)

		return append(b, make([]byte, packed)...)
	}

	for _, tc := range []struct {
		name    string
		head    []byte
		size    int64
		checked bool
		want    int64
	}{
		{"unchecked", head(3, 8, 8), 64, false, 10},
		{"checked", head(3, 8, 8), 64, true, 14},
		{"empty", nil, 64, false, -1},
		{"count only", binary.AppendUvarint(nil, 3), 64, false, -1},
		{"length past object", head(3, 8, 8), 5, false, -1},
		{"no room for checksum", head(3, 8, 8), 11, true, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := sharedDictHeadLen(tc.head, tc.size, tc.checked)
			if tc.want < 0 {
				require.ErrorIs(t, err, ErrCorrupt)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// shortReadBackend truncates every ranged read of one key, standing in for a backend that returns
// fewer bytes than asked for without erroring — which a reader sizing later parses by what it
// requested would follow off the end of what it actually holds.
type shortReadBackend struct {
	backend.Backend

	key string
	max int64
}

func (b *shortReadBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if key == b.key {
		n = min(n, b.max)
	}

	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func TestColumnBlocksRejectsShortDictionaryRead(t *testing.T) {
	t.Parallel()

	const (
		granules = 16
		rows     = 2048
	)

	ctx := context.Background()

	vals := make([][]byte, 0, granules*rows)
	for g := range granules {
		for r := range rows {
			vals = append(vals, fmt.Appendf(nil, "shared-%08d-%s", g*(rows/2)+r/2, "padding-to-defeat-compression"))
		}
	}

	r, b := writeLeadingBytesPart(t, vals, rows)

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.SharedDict)

	short := &shortReadBackend{Backend: b, key: columnKey("p", r.byName["attrs"]), max: dirProbeBytes}

	_, _, err := readSharedDict(ctx, short, short.key, zstdComp(), desc.Bytes, desc.Checked, unlimited)
	require.ErrorIs(t, err, ErrCorrupt)
}

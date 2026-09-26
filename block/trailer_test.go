package block

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func lz4Comp() *compress.Compressor {
	return compress.NewCompressor(compress.AlgorithmLZ4, compress.LevelDefault)
}

func comps() []*compress.Compressor {
	return []*compress.Compressor{noneComp(), lz4Comp(), zstdComp()}
}

const transitionGranule = 64

// transitionValues is a column whose dictionary crosses 256 entries mid-column: granules 0-7 each
// add 32 values (narrow up to 256), granule 8 declines, granule 9 adds 32 more (wide from there),
// granule 10 reuses old values at the wide width, and granule 11 declines.
func transitionValues() [][]byte {
	var vals [][]byte

	for g := range 12 {
		for i := range transitionGranule {
			switch g {
			case 8, 11:
				vals = append(vals, fmt.Appendf(nil, "self-%d-%d", g, i))
			case 9:
				vals = append(vals, fmt.Appendf(nil, "v-%d", 256+i%32))
			case 10:
				vals = append(vals, fmt.Appendf(nil, "v-%d", i%32))
			default:
				vals = append(vals, fmt.Appendf(nil, "v-%d", g*32+i%32))
			}
		}
	}

	return vals
}

// buildBytes builds a block-framed bytes column under l with sizing, recording its object size as a
// part writer does.
func buildBytes(tb testing.TB, vals [][]byte, comp *compress.Compressor, l columnLayout) (ColumnDesc, []byte) {
	tb.Helper()

	l.sizing = true

	desc, obj, err := buildColumnWith(Column{Name: "c", Kind: KindBytes, Bytes: vals, Block: true}, comp, l)
	require.NoError(tb, err)

	desc.Bytes = int64(len(obj))

	return desc, obj
}

// granuleModes reads each granule's mode byte.
func granuleModes(t *testing.T, desc ColumnDesc, obj []byte, comp *compress.Compressor) []byte {
	t.Helper()

	r := newColumnReader(desc, obj, comp, 0)
	dir, err := r.blockDir()
	require.NoError(t, err)

	s := newBlockStreams(dir, comp)
	modes := make([]byte, dir.nBlocks())

	for g := range modes {
		stream, err := s.granule(g)
		require.NoError(t, err)

		modes[g] = stream[0]
	}

	return modes
}

// readAllPaths decodes a bytes column through every whole-object read path and checks each against
// want.
func readAllPaths(t *testing.T, desc ColumnDesc, obj []byte, comp *compress.Compressor, want [][]byte) {
	t.Helper()

	r := func() *ColumnReader { return newColumnReader(desc, obj, comp, len(want)) }

	whole, err := r().Bytes()
	require.NoError(t, err)
	requireRows(t, want, whole, "whole")

	if !desc.Blocked {
		return
	}

	dec, err := r().BlockDecoder()
	require.NoError(t, err)
	checkDecoderRows(t, dec, want, "decoder")

	blocks := []int{0}
	if dec.NumBlocks() > 2 {
		blocks = append(blocks, dec.NumBlocks()-2, dec.NumBlocks()-1)
	}

	scattered, err := r().DecodeBlocksBytesIntoColumn(blocks)
	require.NoError(t, err)

	var packed [][]byte

	for _, g := range blocks {
		lo, hi := dec.BlockSpan(g)
		packed = append(packed, want[lo:hi]...)

		for i := lo; i < hi; i++ {
			require.Equalf(t, want[i], scattered.At(i), "scattered row %d", i)
		}
	}

	got, err := r().DecodeBlocksBytes(blocks)
	require.NoError(t, err)
	requireRows(t, packed, got, "packed")
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:8])
}

// TestTrailerColumnGolden pins the trailer layout's bytes for a column crossing 256 entries and for
// one with an empty dictionary.
func TestTrailerColumnGolden(t *testing.T) {
	t.Parallel()

	vals := transitionValues()
	desc, obj := buildBytes(t, vals, noneComp(), defaultLayout(transitionGranule))

	require.True(t, desc.TrailerDict)
	assert.Equal(t, []byte{2, 2, 2, 2, 2, 2, 2, 2, 1, 3, 3, 1}, granuleModes(t, desc, obj, noneComp()))
	assert.Equal(t, int64(288), desc.DictEntries)
	assert.Equal(t, "ec1e7b8d06dfb8a5", digest(obj))
	assert.Equal(t, trailerDict{off: 2239, length: 1627, raw: 1618, entries: 288},
		trailerDict{off: desc.DictOff, length: desc.DictLen, raw: desc.DictRaw, entries: desc.DictEntries})

	readAllPaths(t, desc, obj, noneComp(), vals)

	dec, err := newColumnReader(desc, obj, noneComp(), len(vals)).BlockDecoder()
	require.NoError(t, err)

	narrow, err := dec.DecodeBytesBlock(0)
	require.NoError(t, err)
	assert.Equal(t, 1, narrow.IDWidth, "a granule sealed before the 257th entry keeps 1-byte ids")

	wide, err := dec.DecodeBytesBlock(9)
	require.NoError(t, err)
	assert.Equal(t, 2, wide.IDWidth)

	t.Run("zero entries", func(t *testing.T) {
		t.Parallel()

		unique := make([][]byte, 3*transitionGranule)
		for i := range unique {
			unique[i] = fmt.Appendf(nil, "unique-%d", i)
		}

		c := Column{Name: "c", Kind: KindBytes, Bytes: unique, Block: true}
		obj, dict, _, ok, err := encodeTrailerDictBytes(c, noneComp(), defaultLayout(transitionGranule), true)
		require.NoError(t, err)
		require.True(t, ok)

		desc := ColumnDesc{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Checked: true, Bytes: int64(len(obj))}
		dict.apply(&desc)

		assert.Equal(t, trailerDict{off: dict.off, length: minDictRegion, raw: 0, entries: 0}, dict)
		assert.Equal(t, "e583175cb606d243", digest(obj))
		assert.Equal(t, []byte{1, 1, 1}, granuleModes(t, desc, obj, noneComp()))

		readAllPaths(t, desc, obj, noneComp(), unique)
	})
}

// TestTrailerMatchesLeading is the differential against the leading layout: the same rows decode to
// the same column, the dictionary region is the leading header byte for byte, and RawBytes is the
// same.
func TestTrailerMatchesLeading(t *testing.T) {
	t.Parallel()

	const (
		granules = 6
		rows     = 256
	)

	corpora := map[string][][]byte{"transition": transitionValues()}
	for _, tc := range sharedDictCases(granules) {
		corpora[tc.name] = mixedSharedValues(granules, rows, tc.self, rows/3)
	}

	for name, vals := range corpora {
		for _, comp := range comps() {
			t.Run(name+"/"+comp.Algorithm().String(), func(t *testing.T) {
				t.Parallel()

				granule := rows
				if name == "transition" {
					granule = transitionGranule
				}

				c := Column{Name: "c", Kind: KindBytes, Bytes: vals, Block: true}
				v2, ok, err := encodeLeadingSharedDict(c, comp, granule, defaultCompressBlockBytes)
				require.NoError(t, err)

				desc, v3 := buildBytes(t, vals, comp, defaultLayout(granule))
				require.Equal(t, ok, desc.TrailerDict)

				if !ok {
					return
				}

				assert.Equal(t, v2[:desc.DictLen], v3[desc.DictOff:desc.DictOff+desc.DictLen])

				want, err := newColumnReader(leadingDesc("c", comp), v2, comp, len(vals)).Bytes()
				require.NoError(t, err)

				got, err := newColumnReader(desc, v3, comp, len(vals)).Bytes()
				require.NoError(t, err)

				assert.Equal(t, want.Entries, got.Entries)
				assert.Equal(t, want.IDWidth, got.IDWidth)
				assert.Equal(t, want.IDs, got.IDs)

				w := NewPartWriter(WithGranuleSize(granule), WithSizingStats())
				require.NoError(t, w.AddColumn(c))

				built, err := w.build()
				require.NoError(t, err)

				m, err := DecodeManifest(built.manifest)
				require.NoError(t, err)
				assert.Equal(t, c.rawBytes(), m.RawBytes)
				assert.Equal(t, manifestVersionExt, m.Version)
			})
		}
	}
}

// trailerObject lays out granule streams as a trailer column over entries, bypassing the writer's
// mode decisions so a test can place any mode.
func trailerObject(t *testing.T, streams [][]byte, entries [][]byte, rows int) (ColumnDesc, []byte) {
	t.Helper()

	acc, err := accumulateBlocked(len(streams)*rows, noneComp(), rows, defaultCompressBlockBytes,
		func(dst []byte, lo, _ int) ([]byte, error) { return append(dst, streams[lo/rows]...), nil })
	require.NoError(t, err)

	var blob []byte
	for _, e := range entries {
		blob = binary.AppendUvarint(blob, uint64(len(e)))
		blob = append(blob, e...)
	}

	region := dictRegion(noneComp(), blob, len(entries))
	obj := acc.finishBuffered(rows, region)

	desc := ColumnDesc{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Checked: true, Bytes: int64(len(obj))}
	trailerDict{off: int64(acc.bytes), length: int64(len(region)), raw: int64(len(blob)), entries: int64(len(entries))}.apply(&desc)

	return desc, obj
}

func nEntries(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = fmt.Appendf(nil, "e%d", i)
	}

	return out
}

// requireCorruptEverywhere asserts every whole-object bytes read path rejects the column.
func requireCorruptEverywhere(t *testing.T, desc ColumnDesc, obj []byte, rows int) {
	t.Helper()

	r := func() *ColumnReader { return newColumnReader(desc, obj, noneComp(), rows) }

	_, err := r().Bytes()
	require.ErrorIs(t, err, ErrCorrupt, "Bytes")

	_, err = r().DecodeBlocksBytesIntoColumn([]int{0})
	require.ErrorIs(t, err, ErrCorrupt, "DecodeBlocksBytesIntoColumn")

	if dec, err := r().BlockDecoder(); err == nil {
		_, err = dec.DecodeBytesBlock(0)
		require.ErrorIs(t, err, ErrCorrupt, "DecodeBytesBlock")
	} else {
		require.ErrorIs(t, err, ErrCorrupt, "BlockDecoder")
	}
}

func TestTrailerGrammarRejections(t *testing.T) {
	t.Parallel()

	const rows = 4

	ids := func(mode byte, id ...byte) []byte { return append([]byte{mode}, id...) }

	for _, tc := range []struct {
		name    string
		streams [][]byte
		entries int
	}{
		{"leading mode in a trailer", [][]byte{ids(modeShared, 0, 1, 0, 1)}, 2},
		{"wide granule over 256 entries or fewer", [][]byte{ids(modeSharedWide, 0, 0, 0, 1, 0, 0, 0, 1)}, 256},
		{"narrow id past the dictionary", [][]byte{ids(modeSharedNarrow, 0, 1, 2, 3)}, 3},
		{"wide id past the dictionary", [][]byte{ids(modeSharedWide, 0, 0, 1, 1, 0, 0, 0, 0)}, 257},
		{"short id array", [][]byte{ids(modeSharedNarrow, 0, 1, 0)}, 2},
		{"unknown mode", [][]byte{ids(4, 0, 1, 0, 1)}, 2},
		{"empty granule", [][]byte{{}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			desc, obj := trailerObject(t, tc.streams, nEntries(tc.entries), rows)
			requireCorruptEverywhere(t, desc, obj, rows)
		})
	}

	t.Run("narrow granule over 256 entries", func(t *testing.T) {
		t.Parallel()

		desc, obj := trailerObject(t, [][]byte{ids(modeSharedNarrow, 0, 255, 1, 255)}, nEntries(300), rows)

		got, err := newColumnReader(desc, obj, noneComp(), rows).Bytes()
		require.NoError(t, err)
		require.Equal(t, 2, got.IDWidth, "a narrow granule widens to the dictionary's width")
		assert.Equal(t, []byte("e255"), got.At(1))
	})

	for _, mode := range []byte{modeSharedNarrow, modeSharedWide} {
		t.Run(fmt.Sprintf("trailer mode %d in a leading column", mode), func(t *testing.T) {
			t.Parallel()

			body, err := encodeBlockedWith(rows, noneComp(), rows, defaultCompressBlockBytes,
				func(dst []byte, _, _ int) ([]byte, error) { return append(dst, mode, 0, 0, 0, 0, 0, 0, 0, 0), nil })
			require.NoError(t, err)

			obj := append(dictRegion(noneComp(), []byte{1, 'a'}, 1), body...)
			requireCorruptEverywhere(t, leadingDesc("c", noneComp()), obj, rows)
		})
	}
}

func TestTrailerLayoutRejections(t *testing.T) {
	t.Parallel()

	vals := transitionValues()
	desc, obj := buildBytes(t, vals, noneComp(), defaultLayout(transitionGranule))

	for _, tc := range []struct {
		name   string
		mutate func(*ColumnDesc, []byte) []byte
	}{
		{"DictOff early", func(d *ColumnDesc, o []byte) []byte { d.DictOff--; return o }},
		{"DictOff late", func(d *ColumnDesc, o []byte) []byte { d.DictOff++; return o }},
		{"DictLen short", func(d *ColumnDesc, o []byte) []byte { d.DictLen--; return o }},
		{"DictEntries off", func(d *ColumnDesc, o []byte) []byte { d.DictEntries--; return o }},
		{"DictRaw understated", func(d *ColumnDesc, o []byte) []byte { d.DictRaw--; return o }},
		{"DictRaw overstated", func(d *ColumnDesc, o []byte) []byte { d.DictRaw++; return o }},
		{"footer length", func(_ *ColumnDesc, o []byte) []byte { o[len(o)-4]++; return o }},
		{"region past the object", func(d *ColumnDesc, o []byte) []byte { d.DictOff = int64(len(o)); return o }},
		{"dictionary checksum", func(d *ColumnDesc, o []byte) []byte { o[d.DictOff+d.DictLen-1]++; return o }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := desc
			o := tc.mutate(&d, append([]byte(nil), obj...))
			requireCorruptEverywhere(t, d, o, len(vals))
		})
	}
}

// TestTrailerRangedRejections covers the ranged reader's own checks: the object must be as long as
// the manifest says, and the tail must hold the recorded dictionary and directory.
func TestTrailerRangedRejections(t *testing.T) {
	t.Parallel()

	vals := transitionValues()

	for _, tc := range []struct {
		name   string
		mutate func(obj []byte) []byte
		// A ranged read of a grown object reads the recorded extent, which is intact.
		ranged bool
	}{
		{"object grew", func(obj []byte) []byte { return append(obj, 0) }, false},
		{"object shrank", func(obj []byte) []byte { return obj[:len(obj)-1] }, true},
		{"footer length", func(obj []byte) []byte { obj[len(obj)-4]++; return obj }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, b := writeBytesPart(t, vals, transitionGranule, WithSizingStats())
			ctx := context.Background()
			key := columnKey("p", 0)

			obj, err := b.Read(ctx, key)
			require.NoError(t, err)
			require.NoError(t, b.Write(ctx, key, tc.mutate(append([]byte(nil), obj...))))

			if tc.ranged {
				_, err = r.ColumnBlocks(ctx, "attrs")
				require.ErrorIs(t, err, ErrCorrupt, "ranged")
			}

			if col, err := r.Column(ctx, "attrs"); err == nil {
				_, err = col.Bytes()
				require.ErrorIs(t, err, ErrCorrupt, "whole")
			} else {
				require.ErrorIs(t, err, ErrCorrupt, "whole")
			}
		})
	}
}

// TestDecompressOverrunIsCorrupt covers a dictionary and a frame decompressing past what the
// manifest and directory record, under every algorithm.
func TestDecompressOverrunIsCorrupt(t *testing.T) {
	t.Parallel()

	vals := transitionValues()

	for _, comp := range comps() {
		t.Run(comp.Algorithm().String(), func(t *testing.T) {
			t.Parallel()

			desc, obj := buildBytes(t, vals, comp, defaultLayout(transitionGranule))
			desc.DictRaw--

			_, err := newColumnReader(desc, obj, comp, len(vals)).Bytes()
			require.ErrorIs(t, err, ErrCorrupt)
			require.ErrorIs(t, err, compress.ErrLimit, "a dictionary past DictRaw")

			ints := make([]int64, 4096)
			for i := range ints {
				ints[i] = int64(i * i)
			}

			acc, err := accumulateColumn(Column{Kind: KindInt64, Int64: ints}, chunk.CodecT64, 0, comp, 512, 1024)
			require.NoError(t, err)

			acc.gLens[len(acc.gLens)-1]-- // the directory understates the last frame by a byte

			frameDesc := ColumnDesc{Name: "i", Kind: KindInt64, Codec: chunk.CodecT64, Blocked: true, Framed: true, Checked: true}
			_, err = newColumnReader(frameDesc, acc.finishBuffered(512, nil), comp, len(ints)).Int64(nil)
			require.ErrorIs(t, err, ErrCorrupt)
			require.ErrorIs(t, err, compress.ErrLimit, "a frame past its granules")
		})
	}
}

// TestSharedDictCapChargesNewValuesOnly: a granule repeating a 17 MiB value already in the
// dictionary adds only its new 1-byte value, so both granules join under a 32 MiB cap. Charging the
// repeat again would put the second granule at 34 MiB and decline it.
func TestSharedDictCapChargesNewValuesOnly(t *testing.T) {
	t.Parallel()

	big := make([]byte, 17<<20)
	a, bv := big, []byte("b")
	vals := [][]byte{a, a, a, a, a, a, a, bv}

	b := newSharedDictBuilder(defaultSharedDictBytes)
	defer b.release()

	ids := make([]int32, 4)
	require.True(t, b.addValues(Column{Kind: KindBytes, Bytes: vals}, 0, 4, ids))
	require.True(t, b.addValues(Column{Kind: KindBytes, Bytes: vals}, 4, 8, ids))
	assert.Equal(t, [][]byte{a, bv}, b.entries)
	assert.Equal(t, []int32{0, 0, 0, 1}, ids)

	tight := newSharedDictBuilder(17 << 20)
	defer tight.release()

	require.False(t, tight.addValues(Column{Kind: KindBytes, Bytes: vals}, 0, 4, ids), "a value past the cap declines")
	require.True(t, tight.addValues(Column{Kind: KindBytes, Bytes: [][]byte{bv, bv}}, 0, 2, ids[:2]),
		"a later granule that fits still joins")
}

// TestSharedDictCapBindsOnlyWhenReached: below the cap the decisions are the uncapped ones, which
// is today's rule; a cap that binds turns exactly the granules past it into self granules.
func TestSharedDictCapBindsOnlyWhenReached(t *testing.T) {
	t.Parallel()

	const granules, rows = 8, 256

	vals := mixedSharedValues(granules, rows, map[int]bool{3: true}, rows)
	c := Column{Kind: KindBytes, Bytes: vals}

	decide := func(dictCap int64, split bool) ([]bool, [][]byte) {
		b := newSharedDictBuilder(dictCap)
		defer b.release()

		sc := c
		if split {
			sc = Column{Kind: KindBytes, BytesDict: nil}
			sc.BytesDict, sc.BytesIDs = splitOf(vals)
		}

		joined := make([]bool, granules)
		ids := make([]int32, rows)

		for g := range joined {
			if split {
				joined[g] = b.addIDs(sc.BytesDict, sc.BytesIDs, g*rows, (g+1)*rows, ids)
			} else {
				joined[g] = b.addValues(sc, g*rows, (g+1)*rows, ids)
			}
		}

		return joined, b.entries
	}

	for _, split := range []bool{false, true} {
		uncapped, all := decide(math.MaxInt64, split)
		capped, entries := decide(defaultSharedDictBytes, split)
		assert.Equal(t, uncapped, capped)
		assert.Equal(t, all, entries)

		var charge int64
		for _, e := range all[:len(all)/2] {
			_, c := entryCharge(e)
			charge += c
		}

		binding, _ := decide(charge, split)
		assert.Contains(t, binding, false)
		assert.Equal(t, uncapped[:2], binding[:2], "granules before the cap binds are unchanged")
		assert.False(t, binding[len(binding)-1], "granules past the cap decline")
	}
}

func splitOf(vals [][]byte) ([][]byte, []int32) {
	index := map[string]int32{}

	var dict [][]byte

	ids := make([]int32, 0, len(vals))

	for _, v := range vals {
		id, ok := index[string(v)]
		if !ok {
			id = int32(len(dict))
			index[string(v)] = id
			dict = append(dict, v)
		}

		ids = append(ids, id)
	}

	return dict, ids
}

// TestSharedDictZeroFilled17MiB: a dictionary compressing far past any fixed ratio still reads
// back, whole and ranged. Its bound is the recorded DictRaw, not the compressed size.
func TestSharedDictZeroFilled17MiB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	zero := make([]byte, 17<<20)
	vals := [][]byte{zero, zero, zero, zero, []byte("x"), []byte("x"), zero, zero}

	r, _ := writeBytesPart(t, vals, 4, WithSizingStats())

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.TrailerDict)
	require.Greater(t, desc.DictRaw/desc.DictLen, int64(256), "the dictionary compresses past 256x")

	col, err := r.Column(ctx, "attrs")
	require.NoError(t, err)

	whole, err := col.Bytes()
	require.NoError(t, err)
	requireRows(t, vals, whole, "whole")

	d, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)
	checkDecoderRows(t, d, vals, "ranged")
}

func TestWithSharedDictBytesClamps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want int64 }{
		{-1, 0},
		{0, 0},
		{512 << 10, 512 << 10},
		{maxSharedDictRaw + 1, maxSharedDictRaw},
		{math.MaxInt64, maxSharedDictRaw},
	} {
		assert.Equal(t, tc.want, newPartConfig([]PartOption{WithSharedDictBytes(tc.in)}).dictCap, "%d", tc.in)
	}

	assert.Equal(t, int64(defaultSharedDictBytes), newPartConfig(nil).dictCap)

	// A zero cap admits nothing, so the column falls back to one unframed stream.
	vals := mixedSharedValues(4, 64, nil, 64)
	w := NewPartWriter(WithGranuleSize(64), WithSharedDictBytes(0))
	require.NoError(t, w.AddColumn(Column{Name: "c", Kind: KindBytes, Bytes: vals, Block: true}))

	built, err := w.build()
	require.NoError(t, err)

	m, err := DecodeManifest(built.manifest)
	require.NoError(t, err)
	assert.False(t, m.Columns[0].Blocked)
	assert.Equal(t, manifestVersionChecked, m.Version, "no extension field, so a version-2 manifest")
}

// FuzzTrailerRoundTrip writes fuzz-shaped rows through PartWriter and reads them back through every
// path, against the leading layout's decode of the same rows.
func FuzzTrailerRoundTrip(f *testing.F) {
	f.Add(uint32(0b0000), 64, 4, 8, uint8(0), false)
	f.Add(uint32(0b0101), 32, 12, 30, uint8(2), true)
	f.Add(uint32(0b1111), 16, 20, 16, uint8(1), false)
	f.Add(uint32(0b0010), 8, 40, 7, uint8(2), true)

	ctx := context.Background()

	f.Fuzz(func(t *testing.T, self uint32, rows, granules, distinct int, alg uint8, tight bool) {
		if rows < 2 || rows > 256 || granules < 1 || granules > 40 || distinct < 1 || distinct > rows {
			t.Skip()
		}

		selfSet := map[int]bool{}
		for g := range granules {
			if self&(1<<(g%32)) != 0 {
				selfSet[g] = true
			}
		}

		vals := mixedSharedValues(granules, rows, selfSet, distinct)
		opts := []PartOption{
			WithGranuleSize(rows), WithSizingStats(), WithCompressBlockBytes(256),
			WithCompression(compress.Algorithm(alg % 3)),
		}

		if tight {
			opts = append(opts, WithSharedDictBytes(int64(rows)*200))
		}

		b := backend.Memory()
		w := NewPartWriter(opts...)
		require.NoError(t, w.AddColumn(Column{Name: "attrs", Kind: KindBytes, Bytes: vals, Block: true}))
		require.NoError(t, WritePart(ctx, b, "p", w))

		r, err := OpenPart(ctx, b, "p")
		require.NoError(t, err)

		col, err := r.Column(ctx, "attrs")
		require.NoError(t, err)

		whole, err := col.Bytes()
		require.NoError(t, err)
		requireRows(t, vals, whole, "whole")

		desc, _ := r.ColumnDescByName("attrs")
		if !desc.Blocked {
			return
		}

		for _, open := range []func() (*Decoder, error){
			func() (*Decoder, error) { return r.ColumnBlocks(ctx, "attrs") },
			func() (*Decoder, error) { return r.ColumnScan(ctx, "attrs", 512) },
		} {
			d, err := open()
			require.NoError(t, err)
			checkDecoderRows(t, d, vals, "decoder")
		}
	})
}

// FuzzTrailerColumnDecode mutates a real trailer column's object and its dictionary placement: every
// read path must error or return rows it can read, never panic.
func FuzzTrailerColumnDecode(f *testing.F) {
	vals := transitionValues()
	desc, obj := buildBytes(f, vals, noneComp(), defaultLayout(transitionGranule))

	f.Add(obj, desc.DictOff, desc.DictLen)
	f.Add(obj[:len(obj)/2], desc.DictOff, desc.DictLen)
	f.Add(obj, desc.DictOff+1, desc.DictLen-1)

	read := func(col *chunk.DictColumn, err error) {
		if err != nil {
			if !errors.Is(err, ErrCorrupt) && col != nil {
				panic("a column alongside an error")
			}

			return
		}

		for i := range col.Len() {
			_ = col.At(i)
		}
	}

	f.Fuzz(func(_ *testing.T, object []byte, off, length int64) {
		d := desc
		d.DictOff, d.DictLen, d.Bytes = off, length, int64(len(object))

		if off < 0 || length < 0 || off > int64(len(object)) || length > int64(len(object))-off {
			return
		}

		read(newColumnReader(d, object, noneComp(), len(vals)).Bytes())
		read(newColumnReader(d, object, noneComp(), len(vals)).DecodeBlocksBytesIntoColumn([]int{0, 9, 11}))

		if dec, err := newColumnReader(d, object, noneComp(), len(vals)).BlockDecoder(); err == nil {
			read(dec.DecodeBytesBlock(9))
		}
	})
}

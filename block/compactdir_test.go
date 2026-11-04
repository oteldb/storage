package block

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func compactDirectoryFixture(tb testing.TB, alg compress.Algorithm, streamed bool) (ColumnDesc, []byte, decodeLimits) {
	tb.Helper()
	ctx := tb.Context()
	be := backend.Memory()
	opts := []PartOption{WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"), WithCompression(alg)}
	w := NewStreamWriter(opts...)
	if streamed {
		w = NewStreamWriterTo(ctx, be, "p", opts...)
	}
	defer w.Abort()
	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Block: true}))
	for range 128 {
		require.NoError(tb, w.AppendInt64(0, []int64{1, 2, 3, 4}))
		require.NoError(tb, w.EndGroup())
	}
	require.NoError(tb, WriteStreamPart(ctx, be, "p", w))
	r, err := OpenPart(ctx, be, "p")
	require.NoError(tb, err)
	desc, ok := r.ColumnDescByName("ts")
	require.True(tb, ok)
	raw, err := be.Read(ctx, columnKey("p", 0))
	require.NoError(tb, err)
	return desc, raw, columnLimits(desc, r.manifest.RawBytes, r.manifest.RowCount)
}

func TestCompactDirectoryReads(t *testing.T) {
	t.Parallel()
	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()
			for _, streamed := range []bool{false, true} {
				t.Run(layoutName(streamed), func(t *testing.T) {
					t.Parallel()
					desc, raw, lim := compactDirectoryFixture(t, alg, streamed)
					require.True(t, desc.CompactDir)
					be := backend.Memory()
					require.NoError(t, be.Write(t.Context(), "object", raw))
					comp := compress.NewCompressor(alg, 0)
					d, _, err := readBlockDir(t.Context(), be, "object", desc, comp, int64(len(raw)), 0, lim)
					require.NoError(t, err)
					require.Nil(t, d.gFrame)
					require.Nil(t, d.gOff)
					require.Nil(t, d.gLen)
					require.Len(t, d.frameRaw, 128)
					in := sizedInput(desc, int64(lim.rows))
					require.Equal(t, d.residentBytes(), in.DirBytes)
					if desc.CompactDir {
						require.Greater(t, in.OpenBytes, desc.Sizing.DirLen)
					}
					col := newColumnReader(desc, raw, comp, lim.rows)
					values, err := col.Int64(nil)
					require.NoError(t, err)
					require.Len(t, values, lim.rows)
					for i, v := range values {
						require.Equal(t, int64(i%4+1), v)
					}
					decoder, err := col.BlockDecoder()
					require.NoError(t, err)
					for _, b := range []int{0, 17, 127} {
						values, err := decoder.DecodeInt64(b)
						require.NoError(t, err)
						require.Equal(t, []int64{1, 2, 3, 4}, values)
					}
				})
			}
		})
	}
}

func TestCompactDirectoryRejectsCorruption(t *testing.T) {
	t.Parallel()
	desc, raw, lim := compactDirectoryFixture(t, compress.AlgorithmLZ4, true)
	require.True(t, desc.CompactDir)
	n := int(desc.Sizing.DirLen)
	start := len(raw) - footerLenBytes - n
	directory := raw[start : len(raw)-footerLenBytes]
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"checksum", func(b []byte) []byte { b[1] ^= 1; return b }},
		{"oversized raw length", func(b []byte) []byte {
			b = binary.AppendUvarint(b[:0], uint64(compactDirBound(desc.Sizing, true)+1))
			return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
		}},
		{"short raw length", func(b []byte) []byte {
			_, k := binary.Uvarint(b)
			out := binary.AppendUvarint(nil, 4)
			out = append(out, b[k:len(b)-dirCRCBytes]...)
			return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bad := tc.mutate(append([]byte(nil), directory...))
			chk := newDirCheck(desc, lim)
			sizing := *chk.sizing
			sizing.DirLen = int64(len(bad))
			chk.sizing, chk.dirComp = &sizing, compress.NewCompressor(desc.Compress, 0)
			_, err := parseDetachedDir(bad, int64(start), chk)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func FuzzCompactDirectory(f *testing.F) {
	type fixture struct {
		desc       ColumnDesc
		limits     decodeLimits
		frameBytes int64
	}
	cases := make(map[byte]fixture)
	for _, alg := range []compress.Algorithm{compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		desc, raw, lim := compactDirectoryFixture(f, alg, true)
		require.True(f, desc.CompactDir)
		n := int(desc.Sizing.DirLen)
		start := len(raw) - footerLenBytes - n
		cases[byte(alg)] = fixture{desc, lim, int64(start)}
		f.Add(raw[start:len(raw)-footerLenBytes], byte(alg))
	}
	f.Add([]byte{}, byte(compress.AlgorithmLZ4))
	f.Fuzz(func(t *testing.T, raw []byte, algorithm byte) {
		if len(raw) > 8192 {
			t.Skip()
		}
		alg := compress.AlgorithmLZ4
		if algorithm%2 == 1 {
			alg = compress.AlgorithmZSTD
		}
		fixture := cases[byte(alg)]
		fixture.desc.Sizing.DirLen = int64(len(raw))
		chk := newDirCheck(fixture.desc, fixture.limits)
		chk.dirComp = compress.NewCompressor(alg, 0)
		d, err := parseDetachedDir(raw, fixture.frameBytes, chk)
		if err != nil {
			return
		}
		require.EqualValues(t, 512, d.rowStarts[d.granules])
		require.Equal(t, fixture.desc.Sizing.NumGranules, int64(d.granules))
		require.Nil(t, d.gFrame)
	})
}

func TestCompactDirectoryGolden(t *testing.T) {
	t.Parallel()
	desc, raw, _ := compactDirectoryFixture(t, compress.AlgorithmNone, true)
	n := int(desc.Sizing.DirLen)
	directory := raw[len(raw)-footerLenBytes-n : len(raw)-footerLenBytes]
	require.Equal(t, "890a01890abf80010480010106b2ebebbe0600ffffe91f0501006c5f04020604000300ff5a010300e006040006040006040006c56bfb631eb6e14d", hex.EncodeToString(directory))
}

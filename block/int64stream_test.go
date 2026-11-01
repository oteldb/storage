package block

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestInt64CursorAndBound(t *testing.T) {
	t.Parallel()
	for _, codec := range []chunk.Codec{chunk.CodecDoD, chunk.CodecDoDScaled, chunk.CodecT64} {
		for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD, compress.AlgorithmLZ4} {
			t.Run(fmt.Sprintf("%s/%s", codec, algorithm), func(t *testing.T) {
				t.Parallel()
				be := backend.Memory()
				want := make([]int64, 37)
				for i := range want {
					want[i] = 1700000000000 + int64(i*i*1000)
				}
				w := NewPartWriter(WithGranuleSize(8), WithCompressBlockBytes(32), WithCompression(algorithm), WithSizingStats())
				require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: codec, Int64: want, Block: true}))
				require.NoError(t, WritePart(t.Context(), be, "p", w))
				r, err := OpenPart(t.Context(), be, "p", NewReadCompressors())
				require.NoError(t, err)
				retained, temporary, ok := r.Int64ScanBound(t.Context(), "ts")
				require.True(t, ok)
				require.Positive(t, retained)
				require.Positive(t, temporary)
				d, err := r.ColumnBlocks(t.Context(), "ts")
				require.NoError(t, err)
				for b := range d.NumBlocks() {
					cur, err := d.Int64Cursor(b)
					require.NoError(t, err)
					lo, hi := d.BlockSpan(b)
					require.Equal(t, hi-lo, cur.Len())
					for i := lo; i < hi; i++ {
						require.Equal(t, i-lo, cur.Pos())
						got, err := cur.Next()
						require.NoError(t, err)
						require.Equal(t, want[i], got)
					}
					_, err = cur.Next()
					require.Error(t, err)
					require.LessOrEqual(t, d.ResidentBytes()+int64(cap(d.ints))*8, retained)
				}
				clone := d.Clone(t.Context())
				require.Empty(t, clone.ints)
				_, err = d.Int64Cursor(-1)
				require.ErrorIs(t, err, ErrCorrupt)
				_, err = d.Int64Cursor(d.NumBlocks())
				require.ErrorIs(t, err, ErrCorrupt)
				_, _, ok = r.Int64ScanBound(t.Context(), "missing")
				require.False(t, ok)
				column, err := r.Column(t.Context(), "ts")
				require.NoError(t, err)
				whole, err := column.BlockDecoder()
				require.NoError(t, err)
				_, _, ok = whole.Int64StreamBound()
				require.False(t, ok, "a whole-object decoder retains the encoded directory prefix")
			})
		}
	}
}

func TestInt64StreamBoundWithoutSizing(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	values := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	w := NewPartWriter(WithGranuleSize(4), WithCompression(compress.AlgorithmZSTD))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Int64: values, Block: true}))
	require.NoError(t, WritePart(t.Context(), be, "p", w))
	r, err := OpenPart(t.Context(), be, "p", NewReadCompressors())
	require.NoError(t, err)
	_, _, sized := r.Int64ScanBound(t.Context(), "ts")
	require.False(t, sized)
	d, err := r.ColumnBlocks(t.Context(), "ts")
	require.NoError(t, err)
	retained, temporary, bounded := d.Int64StreamBound()
	require.True(t, bounded)
	require.Positive(t, retained)
	require.Positive(t, temporary)
	for b := range d.NumBlocks() {
		_, err := d.Int64Cursor(b)
		require.NoError(t, err)
		require.LessOrEqual(t, d.ResidentBytes()+int64(cap(d.ints))*8, retained)
	}
}

func int64CursorDecoder(codec chunk.Codec, rows int, stream []byte) *Decoder {
	c := compress.NewCompressor(compress.AlgorithmNone, compress.LevelFast)
	data := c.Compress(nil, stream)
	return &Decoder{rows: rows, kind: KindInt64, codec: codec, streams: newBlockStreams(blockDir{
		blockRows: rows, granules: 1, frameOff: []int32{0, int32(len(data))}, data: data, legacyMax: int64(len(stream)),
	}, c)}
}

func TestInt64CursorRejectsWrongRowCount(t *testing.T) {
	t.Parallel()
	for _, codec := range []chunk.Codec{chunk.CodecDoD, chunk.CodecDoDScaled, chunk.CodecT64} {
		t.Run(codec.String(), func(t *testing.T) {
			t.Parallel()
			stream := encodeInt64CursorSeed(codec, []int64{1, 2})
			d := int64CursorDecoder(codec, 3, stream)
			_, err := d.Int64Cursor(0)
			require.ErrorIs(t, err, ErrCorrupt)
			require.Empty(t, d.ints)
		})
	}
}

func encodeInt64CursorSeed(codec chunk.Codec, values []int64) []byte {
	switch codec {
	case chunk.CodecDoD:
		return chunk.EncodeTimestamps(nil, values)
	case chunk.CodecDoDScaled:
		return chunk.EncodeTimestampsScaled(nil, values)
	default:
		return chunk.EncodeIntsT64(nil, values)
	}
}

func FuzzInt64Cursor(f *testing.F) {
	codecs := []chunk.Codec{chunk.CodecDoD, chunk.CodecDoDScaled, chunk.CodecT64}
	for i, codec := range codecs {
		for _, values := range [][]int64{{1}, {1, 2}, {1, 1, 1}, {-20, 0, 10000}} {
			f.Add(encodeInt64CursorSeed(codec, values), uint8(i), uint8(len(values)-1))
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, which, count uint8) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		rows := int(count%64) + 1
		d := int64CursorDecoder(codecs[int(which)%len(codecs)], rows, data)
		cur, err := d.Int64Cursor(0)
		if err != nil {
			return
		}
		require.Equal(t, rows, cur.Len())
		for i := range rows {
			require.Equal(t, i, cur.Pos())
			if _, err := cur.Next(); err != nil {
				return
			}
		}
		_, err = cur.Next()
		require.Error(t, err)
	})
}

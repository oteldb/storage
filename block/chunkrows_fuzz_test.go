package block

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func TestChunkRowsGolden(t *testing.T) {
	t.Parallel()
	a := blockAccum{variable: true, chunkRows: []int{2, 1, 3}, gLens: []int{7, 9, 11}, frameCount: 3}
	for i, crc := range []uint32{0x12345678, 0x89abcdef, 0xdeadbeef} {
		a.frameDir = appendFrameMetadata(a.frameDir, 1, 4+i*2, crc)
	}
	raw := a.encodeDir(4)
	require.Equal(t, "0304030104785634120106efcdab890108efbeadde07090b020103", hex.EncodeToString(raw[:len(raw)-4]))
	require.NoError(t, verifyDirCRC(raw, len(raw)-4))
}

func FuzzVariableChunkDirectory(f *testing.F) {
	w := NewStreamWriter(WithIndependentGroups(), WithGranuleSize(4), WithSortKey("ts"))
	require.NoError(f, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Block: true}))
	require.NoError(f, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecFloatGroups, Block: true}))
	for _, v := range [][]float64{{1, 2}, {3, 4, 5, 6, 7}} {
		times := make([]int64, len(v))
		for i := range times {
			times[i] = int64(i)
		}
		require.NoError(f, w.AppendInt64(0, times))
		require.NoError(f, w.AppendFloat64Group(1, v, true))
		require.NoError(f, w.EndGroup())
	}
	built, err := w.build()
	require.NoError(f, err)
	m, err := DecodeManifest(built.manifest)
	require.NoError(f, err)
	f.Add(built.objects[0], true)
	f.Add(built.objects[1], false)
	f.Add([]byte{}, false)
	f.Fuzz(func(t *testing.T, raw []byte, timestamps bool) {
		if len(raw) > 8192 {
			t.Skip()
		}
		index := 1
		if timestamps {
			index = 0
		}
		desc := m.Columns[index]
		limits := columnLimits(desc, m.RawBytes, m.RowCount)
		d, err := parseBlockDirWith(raw, desc, limits)
		if err != nil {
			return
		}
		_, end := d.span(d.granules-1, m.RowCount)
		require.Equal(t, m.RowCount, end)
		c := newColumnReader(desc, raw, compress.NewCompressor(compress.AlgorithmNone, 0), m.RowCount)
		if timestamps {
			_, _ = c.Int64(nil)
		} else {
			_, _ = c.Float64(nil)
		}
	})
}

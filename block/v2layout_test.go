package block

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func defaultLayout(granule int) columnLayout {
	return columnLayout{blockRows: granule, compressBytes: defaultCompressBlockBytes, dictCap: defaultSharedDictBytes}
}

// encodeLeadingSharedDict writes the leading-dictionary layout a version-2 writer produced, for the
// read-compat tests: the same granule decisions without a byte cap, and every shared granule's ids
// at the width the final dictionary implies. ok is false when no granule joins.
func encodeLeadingSharedDict(c Column, comp *compress.Compressor, blockRows, compressBytes int) ([]byte, bool, error) {
	n := c.rows()
	ids := make([]int32, n)
	joined := make([]bool, (n+blockRows-1)/blockRows)
	used := false

	b := newSharedDictBuilder(math.MaxInt64)
	defer b.release()

	for g := range joined {
		lo := g * blockRows
		hi := min(lo+blockRows, n)

		if c.bytesSplitForm() {
			joined[g] = b.addIDs(c.BytesDict, c.BytesIDs, lo, hi, ids[lo:hi])
		} else {
			joined[g] = b.addValues(c, lo, hi, ids[lo:hi])
		}

		used = used || joined[g]
	}

	if !used {
		return nil, false, nil
	}

	width := sharedIDWidth(b.entries)

	body, err := encodeBlockedWith(n, comp, blockRows, compressBytes, func(dst []byte, lo, hi int) ([]byte, error) {
		if !joined[lo/blockRows] {
			return appendBlockStream(append(dst, modeSelf), c, chunk.CodecDict, 0, lo, hi)
		}

		dst = append(dst, modeShared)

		for _, id := range ids[lo:hi] {
			if width == 1 {
				dst = append(dst, byte(id))
			} else {
				dst = binary.BigEndian.AppendUint16(dst, uint16(id))
			}
		}

		return dst, nil
	})
	if err != nil {
		return nil, false, err
	}

	var blob []byte
	for _, e := range b.entries {
		blob = binary.AppendUvarint(blob, uint64(len(e)))
		blob = append(blob, e...)
	}

	return append(dictRegion(comp, blob, len(b.entries)), body...), true, nil
}

// writeLeadingBytesPart is [writeBytesPart] under the version-2 leading-dictionary layout.
func writeLeadingBytesPart(t *testing.T, vals [][]byte, granule int) (*PartReader, backendtest.SizedByteCounter) {
	t.Helper()

	ctx := context.Background()
	b := backendtest.NewSizedByteCounter(backendtest.NewStreamingMemory())
	comp := zstdComp()
	c := Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Block: true, Bytes: vals}

	obj, ok, err := encodeLeadingSharedDict(c, comp, granule, 1024)
	require.NoError(t, err)
	require.True(t, ok, "no granule joined the dictionary")

	desc := leadingDesc("attrs", comp)
	desc.Bytes = int64(len(obj))

	m := Manifest{
		Version: manifestVersionChecked, RowCount: len(vals), GranuleSize: granule,
		Columns: []ColumnDesc{desc}, RawBytes: c.rawBytes(),
	}

	require.NoError(t, b.Write(ctx, columnKey("p", 0), obj))
	require.NoError(t, b.Write(ctx, marksKey("p"), Marks{GranuleSize: granule}.Encode(nil)))
	require.NoError(t, b.Write(ctx, manifestKey("p"), m.Encode(nil)))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	return r, b
}

// leadingDesc is the descriptor a version-2 writer recorded for a leading shared-dictionary column.
func leadingDesc(name string, comp *compress.Compressor) ColumnDesc {
	return ColumnDesc{
		Name: name, Kind: KindBytes, Codec: chunk.CodecDict, Compress: comp.Algorithm(),
		Blocked: true, Framed: true, SharedDict: true, Checked: true,
	}
}

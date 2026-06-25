package block

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func noneComp() *compress.Compressor {
	return compress.NewCompressor(compress.AlgorithmNone, compress.LevelDefault)
}
func zstdComp() *compress.Compressor {
	return compress.NewCompressor(compress.AlgorithmZSTD, compress.LevelDefault)
}

func TestColumnInt64RoundTrip(t *testing.T) {
	t.Parallel()

	for _, codec := range []chunk.Codec{chunk.CodecT64, chunk.CodecDoD} {
		vals := []int64{5, 7, 9, 2, 100, -3}
		desc, obj, err := buildColumn(Column{Name: "c", Kind: KindInt64, Int64: vals, Codec: codec}, noneComp())
		require.NoError(t, err)
		assert.False(t, desc.Const)
		assert.Equal(t, int64(-3), desc.MinInt64)
		assert.Equal(t, int64(100), desc.MaxInt64)

		r := newColumnReader(desc, obj, noneComp(), len(vals))
		got, err := r.Int64(nil)
		require.NoError(t, err)
		assert.Equal(t, vals, got)
	}
}

func TestColumnFloat64RoundTrip(t *testing.T) {
	t.Parallel()

	for _, codec := range []chunk.Codec{chunk.CodecGorilla, chunk.CodecDecimal} {
		vals := []float64{1.5, 2.5, 1.5, 9.0, -4.25}
		desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, Codec: codec}, noneComp())
		require.NoError(t, err)
		assert.InDelta(t, -4.25, desc.MinFloat64, 0)
		assert.InDelta(t, 9.0, desc.MaxFloat64, 0)

		r := newColumnReader(desc, obj, noneComp(), len(vals))
		got, err := r.Float64(nil)
		require.NoError(t, err)
		assert.Equal(t, vals, got)
	}
}

func TestColumnBytesRoundTrip(t *testing.T) {
	t.Parallel()

	vals := [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("c")}
	desc, obj, err := buildColumn(Column{Name: "k", Kind: KindBytes, Bytes: vals}, noneComp())
	require.NoError(t, err)
	assert.Equal(t, chunk.CodecDict, desc.Codec)
	assert.False(t, desc.Const)

	r := newColumnReader(desc, obj, noneComp(), len(vals))
	dc, err := r.Bytes()
	require.NoError(t, err)
	require.Equal(t, len(vals), dc.Len())
	for i, want := range vals {
		assert.Equal(t, want, dc.At(i))
	}
}

func TestColumnZSTDCompressed(t *testing.T) {
	t.Parallel()

	vals := make([]int64, 1000) // compressible
	for i := range vals {
		vals[i] = int64(i % 7)
	}

	desc, obj, err := buildColumn(Column{Name: "c", Kind: KindInt64, Int64: vals, Codec: chunk.CodecT64}, zstdComp())
	require.NoError(t, err)
	assert.Equal(t, compress.AlgorithmZSTD, desc.Compress)

	got, err := newColumnReader(desc, obj, zstdComp(), len(vals)).Int64(nil)
	require.NoError(t, err)
	assert.Equal(t, vals, got)
}

func TestColumnConstInt64(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "shard", Kind: KindInt64, Int64: []int64{7, 7, 7, 7}}, noneComp())
	require.NoError(t, err)
	assert.True(t, desc.Const, "all-equal int64 collapses")
	assert.Equal(t, int64(7), desc.ConstInt64)
	assert.Nil(t, obj, "constant column emits no object")

	r := newColumnReader(desc, nil, noneComp(), 4)
	v, ok := r.Const()
	assert.True(t, ok)
	assert.Equal(t, int64(7), v)

	got, err := r.Int64(nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{7, 7, 7, 7}, got)
}

func TestColumnConstFloat64NaN(t *testing.T) {
	t.Parallel()

	nan := math.NaN()
	desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: []float64{nan, nan, nan}}, noneComp())
	require.NoError(t, err)
	assert.True(t, desc.Const, "all-NaN (same bits) collapses")
	assert.Nil(t, obj)

	got, err := newColumnReader(desc, nil, noneComp(), 3).Float64(nil)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, v := range got {
		assert.True(t, math.IsNaN(v))
	}
}

func TestColumnConstBytes(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "unit", Kind: KindBytes, Bytes: [][]byte{[]byte("ms"), []byte("ms"), []byte("ms")}}, noneComp())
	require.NoError(t, err)
	assert.True(t, desc.Const)
	assert.Equal(t, []byte("ms"), desc.ConstBytes)
	assert.Nil(t, obj)

	dc, err := newColumnReader(desc, nil, noneComp(), 3).Bytes()
	require.NoError(t, err)
	require.Equal(t, 3, dc.Len())
	for i := range 3 {
		assert.Equal(t, []byte("ms"), dc.At(i))
	}
}

func TestColumnEmpty(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "c", Kind: KindInt64, Int64: nil}, noneComp())
	require.NoError(t, err)
	assert.False(t, desc.Const, "empty column is not constant")

	got, err := newColumnReader(desc, obj, noneComp(), 0).Int64(nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestColumnInvalidKind(t *testing.T) {
	t.Parallel()

	_, _, err := buildColumn(Column{Name: "x", Kind: Kind(99)}, noneComp())
	require.Error(t, err)
}

func TestColumnCodecKindMismatch(t *testing.T) {
	t.Parallel()

	// Gorilla is a float codec; applying it to an int64 column must error.
	_, _, err := buildColumn(Column{Name: "c", Kind: KindInt64, Int64: []int64{1, 2, 3}, Codec: chunk.CodecGorilla}, noneComp())
	require.Error(t, err)
}

func TestColumnWrongAccessor(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: []float64{1, 2, 3}}, noneComp())
	require.NoError(t, err)
	r := newColumnReader(desc, obj, noneComp(), 3)

	_, err = r.Int64(nil)
	require.Error(t, err, "Int64 on a float column must error")
	_, err = r.Bytes()
	require.Error(t, err)

	v, ok := r.Const()
	assert.False(t, ok)
	assert.Nil(t, v)
}

func TestColumnKindAccessors(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "c", Kind: KindInt64, Int64: []int64{1, 2, 9}}, noneComp())
	require.NoError(t, err)
	r := newColumnReader(desc, obj, noneComp(), 3)
	assert.Equal(t, KindInt64, r.Kind())
	assert.Equal(t, 3, r.Len())
}

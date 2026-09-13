package block

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
)

func TestPartObjectSizes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()

	ts := make([]int64, 64)
	vals := make([]float64, 64)
	consts := make([]int64, 64)

	for i := range ts {
		ts[i] = int64(i) * 15_000_000_000
		vals[i] = float64(i) * 1.5
		consts[i] = 7
	}

	w := NewPartWriter(WithSortKey("ts"))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Int64: ts}))
	require.NoError(t, w.AddColumn(Column{Name: "const", Kind: KindInt64, Codec: chunk.CodecT64, Int64: consts}))
	require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Codec: chunk.CodecGorilla, Float64: vals}))
	require.NoError(t, WritePart(ctx, b, "t/p", w))
	require.NoError(t, b.Write(ctx, "t/p/sidx", []byte("index")))
	// A sibling whose prefix extends this part's must not be counted as part of it.
	require.NoError(t, b.Write(ctx, "t/p2/c/0", []byte("sibling")))

	r, err := OpenPart(ctx, b, "t/p")
	require.NoError(t, err)
	require.True(t, r.Manifest().Columns[1].Const, "the constant column collapses and has no object")

	got, err := PartObjectSizes(ctx, b, "t/p")
	require.NoError(t, err)

	size := func(key string) int64 {
		n, err := backend.SizeOf(ctx, b, key)
		require.NoError(t, err)

		return n
	}

	assert.Equal(t, map[int]int64{0: size("t/p/c/0"), 2: size("t/p/c/2")}, got.Columns)
	assert.Equal(t, map[string]int64{
		"manifest": size("t/p/manifest"),
		"marks":    size("t/p/marks"),
		"sidx":     5,
	}, got.Other)

	var sum int64
	for _, n := range got.Columns {
		sum += n
	}

	for _, n := range got.Other {
		sum += n
	}

	assert.Equal(t, got.Total, sum)
}

func TestPartObjectSizesNilBackend(t *testing.T) {
	t.Parallel()

	got, err := PartObjectSizes(context.Background(), nil, "t/p")
	require.NoError(t, err)
	assert.Zero(t, got.Total)
	assert.Empty(t, got.Columns)
	assert.Empty(t, got.Other)
}

func TestColumnIndex(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		want int
		ok   bool
	}{
		{"c/0", 0, true},
		{"c/12", 12, true},
		{"c/", 0, false},
		{"c/x", 0, false},
		{"c/-1", 0, false},
		{"c/+1", 0, false},
		{"c/01", 0, false},
		{"c/1/frames", 0, false},
		{"manifest", 0, false},
		{"cc/1", 0, false},
	} {
		i, ok := columnIndex(tc.name)
		assert.Equal(t, tc.ok, ok, tc.name)
		assert.Equal(t, tc.want, i, tc.name)
	}
}

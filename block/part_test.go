package block

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
)

// samplePartWriter builds a representative part: a DoD timestamp sort key, a Gorilla
// value column, a constant bytes unit, a non-constant bytes name, and a constant shard.
func samplePartWriter(t *testing.T) (*PartWriter, struct {
	ts    []int64
	value []float64
	unit  [][]byte
	name  [][]byte
	shard []int64
},
) {
	t.Helper()

	data := struct {
		ts    []int64
		value []float64
		unit  [][]byte
		name  [][]byte
		shard []int64
	}{
		ts:    []int64{1000, 1015, 1030, 1045, 1060, 1075},
		value: []float64{1.5, 2.5, 1.5, 9.0, -4.25, 7.0},
		unit:  [][]byte{[]byte("ms"), []byte("ms"), []byte("ms"), []byte("ms"), []byte("ms"), []byte("ms")},
		name:  [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("c"), []byte("a"), []byte("b")},
		shard: []int64{3, 3, 3, 3, 3, 3},
	}

	w := NewPartWriter(WithSortKey("ts"), WithGranuleSize(2))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Int64: data.ts, Codec: chunk.CodecDoD}))
	require.NoError(t, w.AddColumn(Column{Name: "value", Kind: KindFloat64, Float64: data.value}))
	require.NoError(t, w.AddColumn(Column{Name: "unit", Kind: KindBytes, Bytes: data.unit}))
	require.NoError(t, w.AddColumn(Column{Name: "name", Kind: KindBytes, Bytes: data.name}))
	require.NoError(t, w.AddColumn(Column{Name: "shard", Kind: KindInt64, Int64: data.shard}))

	return w, data
}

func backendFactories() map[string]func(t *testing.T) backend.Backend {
	return map[string]func(t *testing.T) backend.Backend{
		"memory": func(*testing.T) backend.Backend { return backend.Memory() },
		"file": func(t *testing.T) backend.Backend {
			t.Helper()
			b, err := file.New(t.TempDir())
			require.NoError(t, err)

			return b
		},
	}
}

// TestPartRoundTripBackends is the M1 exit criterion: write a part to, and read it back
// from, both the memory and file backends through one code path.
func TestPartRoundTripBackends(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for name, factory := range backendFactories() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := factory(t)
			w, data := samplePartWriter(t)
			require.NoError(t, WritePart(ctx, b, "metrics/0", w))

			r, err := OpenPart(ctx, b, "metrics/0")
			require.NoError(t, err)
			assert.Equal(t, 6, r.RowCount())
			assert.Equal(t, []string{"ts", "value", "unit", "name", "shard"}, r.ColumnNames())

			ts, err := mustColInt64(ctx, t, r, "ts")
			require.NoError(t, err)
			assert.Equal(t, data.ts, ts)

			val, err := mustColFloat64(ctx, t, r, "value")
			require.NoError(t, err)
			assert.Equal(t, data.value, val)

			unit := mustColBytes(ctx, t, r, "unit")
			for i := range data.unit {
				assert.Equal(t, data.unit[i], unit.At(i))
			}

			gotName := mustColBytes(ctx, t, r, "name")
			for i := range data.name {
				assert.Equal(t, data.name[i], gotName.At(i))
			}

			shard, err := mustColInt64(ctx, t, r, "shard")
			require.NoError(t, err)
			assert.Equal(t, data.shard, shard)

			// Marks: 6 rows / granule 2 = 3 granules; pruning works.
			marks, err := r.Marks(ctx)
			require.NoError(t, err)
			require.Len(t, marks.Granules, 3)
			assert.Len(t, marks.Overlapping(1028, 1032), 1, "only the middle granule covers [1028,1032]")
		})
	}
}

// TestPartObjectLayout checks the multi-key layout and that constant columns emit no
// data object.
func TestPartObjectLayout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	w, _ := samplePartWriter(t)
	require.NoError(t, WritePart(ctx, b, "p", w))

	keys, err := b.List(ctx, "p/")
	require.NoError(t, err)
	// const columns unit(ord 2) and shard(ord 4) emit no c/ object.
	assert.Equal(t, []string{"p/c/0", "p/c/1", "p/c/3", "p/manifest", "p/marks"}, keys)
}

func TestOpenPartMissingManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, err := OpenPart(ctx, backend.Memory(), "nope")
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrNotExist, "a part without a manifest is not readable")
}

func TestOpenPartCorruptManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	require.NoError(t, b.Write(ctx, "p/manifest", []byte("garbage")))

	_, err := OpenPart(ctx, b, "p")
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestPartAddColumnRowMismatch(t *testing.T) {
	t.Parallel()

	w := NewPartWriter()
	require.NoError(t, w.AddColumn(Column{Name: "a", Kind: KindInt64, Int64: []int64{1, 2, 3}}))
	err := w.AddColumn(Column{Name: "b", Kind: KindInt64, Int64: []int64{1, 2}})
	require.Error(t, err, "row counts must match")
}

func TestPartBuildNoColumns(t *testing.T) {
	t.Parallel()

	err := WritePart(context.Background(), backend.Memory(), "p", NewPartWriter())
	require.Error(t, err)
}

func TestPartInvalidColumnRejected(t *testing.T) {
	t.Parallel()

	w := NewPartWriter()
	require.Error(t, w.AddColumn(Column{Name: "x", Kind: Kind(42)}))

	// A codec that doesn't match the kind fails at build/write time. The values must be
	// non-constant, else the column collapses to a const and the codec is never used.
	w2 := NewPartWriter()
	require.NoError(t, w2.AddColumn(Column{Name: "v", Kind: KindFloat64, Float64: []float64{1, 2, 3}, Codec: chunk.CodecT64}))
	require.Error(t, WritePart(context.Background(), backend.Memory(), "p", w2))
}

func TestPartColumnNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backend.Memory()
	w, _ := samplePartWriter(t)
	require.NoError(t, WritePart(ctx, b, "p", w))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)
	_, err = r.Column(ctx, "missing")
	require.Error(t, err)
}

// FuzzOpenPart feeds arbitrary bytes as a part's manifest object: OpenPart must return
// an error or a valid reader, never panic.
func FuzzOpenPart(f *testing.F) {
	f.Add(sampleManifest().Encode(nil))
	f.Add([]byte("garbage"))
	f.Add([]byte{})

	ctx := context.Background()

	f.Fuzz(func(t *testing.T, manifest []byte) {
		b := backend.Memory()
		require.NoError(t, b.Write(ctx, "p/manifest", manifest))

		r, err := OpenPart(ctx, b, "p")
		if err != nil {
			return
		}
		// A reader that opened must expose a consistent row count and column set.
		assert.Len(t, r.ColumnNames(), len(r.Manifest().Columns))
	})
}

func mustColInt64(ctx context.Context, t *testing.T, r *PartReader, name string) ([]int64, error) {
	t.Helper()
	c, err := r.Column(ctx, name)
	require.NoError(t, err)

	return c.Int64(nil)
}

func mustColFloat64(ctx context.Context, t *testing.T, r *PartReader, name string) ([]float64, error) {
	t.Helper()
	c, err := r.Column(ctx, name)
	require.NoError(t, err)

	return c.Float64(nil)
}

func mustColBytes(ctx context.Context, t *testing.T, r *PartReader, name string) *chunk.DictColumn {
	t.Helper()
	c, err := r.Column(ctx, name)
	require.NoError(t, err)
	dc, err := c.Bytes()
	require.NoError(t, err)

	return dc
}

func BenchmarkPartWrite(b *testing.B) {
	ctx := context.Background()
	w, _ := buildBenchWriter()
	bk := backend.Memory()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_ = WritePart(ctx, bk, "p", w)
	}
}

func BenchmarkPartReadColumn(b *testing.B) {
	ctx := context.Background()
	w, _ := buildBenchWriter()
	bk := backend.Memory()
	_ = WritePart(ctx, bk, "p", w)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		r, _ := OpenPart(ctx, bk, "p")
		c, _ := r.Column(ctx, "value")
		_, _ = c.Float64(nil)
	}
}

func buildBenchWriter() (*PartWriter, int) {
	const n = 1000

	ts := make([]int64, n)
	val := make([]float64, n)

	for i := range n {
		ts[i] = int64(1_000_000_000 + i*15000)
		val[i] = float64(i) * 0.5
	}

	w := NewPartWriter(WithSortKey("ts"))
	_ = w.AddColumn(Column{Name: "ts", Kind: KindInt64, Int64: ts, Codec: chunk.CodecDoD})
	_ = w.AddColumn(Column{Name: "value", Kind: KindFloat64, Float64: val})

	return w, n
}

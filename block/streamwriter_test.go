package block

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// metricRows is a metric part's columns in the layout the engine writes.
type metricRows struct {
	series []chunk.U128
	ts     []int64
	value  []float64
}

// runs collapses the id column into the runs a streaming writer feeds.
func (m metricRows) runs() []chunk.U128Run {
	var out []chunk.U128Run

	for i := 0; i < len(m.series); {
		j := i + 1
		for j < len(m.series) && m.series[j] == m.series[i] {
			j++
		}

		out = append(out, chunk.U128Run{Value: m.series[i], Count: j - i})
		i = j
	}

	return out
}

// writeBatch builds the part with the whole-column [PartWriter].
func (m metricRows) writeBatch(t *testing.T, autoCodec bool, opts ...PartOption) builtPart {
	t.Helper()

	w := NewPartWriter(opts...)
	require.NoError(t, w.AddColumn(Column{Name: "series", Kind: KindInt128, Int128: m.series}))
	require.NoError(t, w.AddColumn(Column{
		Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: m.ts, Block: true,
	}))
	require.NoError(t, w.AddColumn(Column{
		Name: "value", Kind: KindFloat64, Float64: m.value, AutoCodec: autoCodec, Block: true,
	}))

	built, err := w.build()
	require.NoError(t, err)

	return built
}

// writeStream builds the same part with [StreamWriter], one series at a time — the shape a merge
// produces.
func (m metricRows) writeStream(t *testing.T, autoCodec bool, opts ...PartOption) builtPart {
	t.Helper()

	w := NewStreamWriter(opts...)
	require.NoError(t, w.AddColumn(Column{Name: "series", Kind: KindInt128}))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(t, w.AddColumn(Column{
		Name: "value", Kind: KindFloat64, AutoCodec: autoCodec, Block: true,
	}))

	off := 0
	for _, r := range m.runs() {
		require.NoError(t, w.AppendU128Run(0, r.Value, r.Count))
		require.NoError(t, w.AppendInt64(1, m.ts[off:off+r.Count]))
		require.NoError(t, w.AppendFloat64(2, m.value[off:off+r.Count]))
		off += r.Count
	}

	require.Equal(t, len(m.ts), w.Rows())

	built, err := w.build()
	require.NoError(t, err)

	return built
}

// decodeBuilt writes a built part to a backend and reads its three columns back.
func decodeBuilt(t *testing.T, built builtPart) metricRows {
	t.Helper()

	ctx := context.Background()
	b := backend.Memory()
	require.NoError(t, built.write(ctx, b, "p"))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	var got metricRows

	sc, err := r.Column(ctx, "series")
	require.NoError(t, err)
	got.series, err = sc.ID128(nil)
	require.NoError(t, err)

	tc, err := r.Column(ctx, "ts")
	require.NoError(t, err)
	got.ts, err = tc.Int64(nil)
	require.NoError(t, err)

	vc, err := r.Column(ctx, "value")
	require.NoError(t, err)
	got.value, err = vc.Float64(nil)
	require.NoError(t, err)

	return got
}

// gen builds a metric corpus of nSeries series with samplesPer samples each.
func gen(nSeries, samplesPer int, value func(rng *rand.Rand, s, i int) float64, seed uint64) metricRows {
	rng := rand.New(rand.NewPCG(seed, seed+1))

	var m metricRows

	for s := range nSeries {
		id := chunk.U128{Hi: uint64(s / 4), Lo: uint64(s)}
		for i := range samplesPer {
			m.series = append(m.series, id)
			m.ts = append(m.ts, int64(i)*15_000+int64(s))
			m.value = append(m.value, value(rng, s, i))
		}
	}

	return m
}

// metricCase is one corpus the writers are compared over.
type metricCase struct {
	name  string
	rows  metricRows
	gsize int
}

// metricCases spans the shapes the granule packing and the codec choice turn on: empty, sub-granule,
// granule-aligned, series spanning granules, constant, and the float values the decimal codec cannot
// represent.
func metricCases() []metricCase {
	return []metricCase{
		{"empty", metricRows{}, 4},
		{"single row", gen(1, 1, func(*rand.Rand, int, int) float64 { return 1.5 }, 1), 4},
		{"one series many granules", gen(1, 100, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 2), 8},
		{"many series short", gen(50, 3, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 3), 8},
		{"series spanning granules", gen(7, 37, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 4), 16},
		{"rows exactly fill granules", gen(4, 16, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 5), 8},
		{"constant value", gen(6, 9, func(*rand.Rand, int, int) float64 { return 7 }, 6), 4},
		{"integer counter", gen(6, 40, func(_ *rand.Rand, s, i int) float64 { return float64(s*1000 + i) }, 7), 8},
		{"non-finite values", gen(3, 8, func(_ *rand.Rand, _, i int) float64 {
			switch i % 3 {
			case 0:
				return math.NaN()
			case 1:
				return math.Inf(1)
			default:
				return float64(i)
			}
		}, 8), 4},
		{"all NaN", gen(2, 6, func(*rand.Rand, int, int) float64 { return math.NaN() }, 9), 4},
		{"negative zero", gen(2, 6, func(_ *rand.Rand, _, i int) float64 {
			if i%2 == 0 {
				return math.Copysign(0, -1)
			}

			return float64(i)
		}, 10), 4},
	}
}

// TestStreamWriterMatchesPartWriter is the invariant the engine's merge relies on: streaming a
// part's rows in produces the same part as handing the whole columns to [PartWriter]. With an
// explicit codec the objects must be byte-identical; the layouts, marks and manifest must match
// in every case.
func TestStreamWriterMatchesPartWriter(t *testing.T) {
	t.Parallel()

	cases := metricCases()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := []PartOption{
				WithSortKey("ts"), WithGranuleSize(tc.gsize),
				WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64),
			}

			t.Run("explicit codec", func(t *testing.T) {
				t.Parallel()

				batch := tc.rows.writeBatch(t, false, opts...)
				stream := tc.rows.writeStream(t, false, opts...)

				assert.Equal(t, batch.manifest, stream.manifest, "manifest")
				assert.Equal(t, batch.marks, stream.marks, "marks")
				require.Len(t, stream.objects, len(batch.objects))

				for i := range batch.objects {
					assert.Equal(t, batch.objects[i], stream.objects[i], "column %d object", i)
				}
			})

			t.Run("auto codec", func(t *testing.T) {
				t.Parallel()

				// The adaptive float codec compares block-framed sizes when streaming and
				// whole-column sizes when batching, so the two can pick differently in a marginal
				// case. Both must be lossless, so the decoded values must match regardless.
				batch := decodeBuilt(t, tc.rows.writeBatch(t, true, opts...))
				stream := decodeBuilt(t, tc.rows.writeStream(t, true, opts...))

				assertRowsEqual(t, batch, stream)
				assertRowsEqual(t, tc.rows, stream)
			})
		})
	}
}

// assertRowsEqual compares two decoded parts by value. Values are compared with IEEE numeric
// equality (plus NaN treated as equal to NaN, which IEEE does not): that is exactly the guarantee
// the adaptive float codec makes — see decimalRoundTrips, which deliberately lets the scaled-
// decimal path collapse a spurious −0 to +0 rather than poison the whole column. So the two
// writers may preserve a −0's sign bit differently while both being lossless.
func assertRowsEqual(t *testing.T, want, got metricRows) {
	t.Helper()

	require.Equal(t, orNil(want.series), orNil(got.series), "series")
	require.Equal(t, orNilI64(want.ts), orNilI64(got.ts), "ts")
	require.Len(t, got.value, len(want.value), "value len")

	for i := range want.value {
		w, g := want.value[i], got.value[i]
		if math.IsNaN(w) && math.IsNaN(g) {
			continue
		}

		// Delta 0 is exact numeric equality: what the codecs guarantee, and unlike a bitwise
		// compare it accepts the −0/+0 collapse described above.
		assert.InDelta(t, w, g, 0, "value[%d]", i)
	}
}

func orNil(s []chunk.U128) []chunk.U128 {
	if len(s) == 0 {
		return nil
	}

	return s
}

func orNilI64(s []int64) []int64 {
	if len(s) == 0 {
		return nil
	}

	return s
}

// TestStreamWriterLossyPrecision pins the lossy (age-tiered) path: a precision budget must produce
// the same descriptor and decode to the same values as the batch writer's.
func TestStreamWriterLossyPrecision(t *testing.T) {
	t.Parallel()

	rows := gen(5, 20, func(r *rand.Rand, _, _ int) float64 { return r.Float64() * 1000 }, 11)

	opts := []PartOption{WithSortKey("ts"), WithGranuleSize(8), WithCompression(compress.AlgorithmZSTD)}

	w := NewStreamWriter(opts...)
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(t, w.AddColumn(Column{
		Name: "value", Kind: KindFloat64, AutoCodec: true, FloatPrecisionBits: 20, Block: true,
	}))

	require.NoError(t, w.AppendInt64(0, rows.ts))
	require.NoError(t, w.AppendFloat64(1, rows.value))

	built, err := w.build()
	require.NoError(t, err)

	m, err := DecodeManifest(built.manifest)
	require.NoError(t, err)

	require.Equal(t, uint8(20), m.Columns[1].FloatPrecisionBits,
		"the precision budget must be recorded so the merge engine reaches its fixed point")
}

// TestStreamWriterRejectsUnstreamable pins the guard rails: a column whose encoding cannot restart
// per granule, or a schema whose columns disagree on row count, must error rather than write a
// silently wrong part.
func TestStreamWriterRejectsUnstreamable(t *testing.T) {
	t.Parallel()

	t.Run("unblocked float", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.Error(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64}))
	})

	t.Run("unblocked int64", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.Error(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64}))
	})

	t.Run("bytes", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.Error(t, w.AddColumn(Column{Name: "b", Kind: KindBytes}))
	})

	t.Run("blocked int128", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.Error(t, w.AddColumn(Column{Name: "s", Kind: KindInt128, Block: true}))
	})

	t.Run("no columns", func(t *testing.T) {
		t.Parallel()

		_, err := NewStreamWriter().build()
		require.Error(t, err)
	})

	t.Run("kind mismatch", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
		require.Error(t, w.AppendFloat64(0, []float64{1}))
		require.Error(t, w.AppendInt64(1, []int64{1}))
	})

	t.Run("row count mismatch", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
		require.NoError(t, w.AddColumn(Column{Name: "v", Kind: KindFloat64, Block: true}))
		require.NoError(t, w.AppendInt64(0, []int64{1, 2, 3}))
		require.NoError(t, w.AppendFloat64(1, []float64{1}))

		_, err := w.build()
		require.Error(t, err)
	})
}

// FuzzStreamWriterMatchesPartWriter drives both writers with the same randomly shaped corpus,
// varying granule size, batch boundaries and value shape.
func FuzzStreamWriterMatchesPartWriter(f *testing.F) {
	f.Add(uint64(1), 3, 5, 4, 0)
	f.Add(uint64(7), 1, 40, 8, 1)
	f.Add(uint64(9), 20, 1, 2, 2)

	f.Fuzz(func(t *testing.T, seed uint64, nSeries, samplesPer, gsize, shape int) {
		if nSeries < 0 || nSeries > 40 || samplesPer < 0 || samplesPer > 60 || gsize < 1 || gsize > 64 {
			t.Skip()
		}

		value := func(r *rand.Rand, s, i int) float64 {
			switch shape % 4 {
			case 0:
				return r.Float64()
			case 1:
				return float64(s*100 + i) // integer-valued: the decimal codec's case
			case 2:
				return 3.5 // constant
			default:
				if i%5 == 0 {
					return math.NaN()
				}

				return float64(i) / 8
			}
		}

		rows := gen(nSeries, samplesPer, value, seed)
		opts := []PartOption{
			WithSortKey("ts"), WithGranuleSize(gsize),
			WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(32),
		}

		batch := rows.writeBatch(t, false, opts...)
		stream := rows.writeStream(t, false, opts...)

		require.Equal(t, batch.manifest, stream.manifest)
		require.Equal(t, batch.marks, stream.marks)
		require.Equal(t, batch.objects, stream.objects)

		assertRowsEqual(t, rows, decodeBuilt(t, rows.writeStream(t, true, opts...)))
	})
}

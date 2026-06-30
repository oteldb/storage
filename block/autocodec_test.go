package block

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// buildAuto builds a float column with adaptive codec selection and decodes it back, returning
// the chosen codec and the decoded values.
func buildAuto(t *testing.T, vals []float64, comp *compress.Compressor) (chunk.Codec, []float64) {
	t.Helper()

	desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, AutoCodec: true}, comp, defaultGranuleSize)
	require.NoError(t, err)

	got, err := newColumnReader(desc, obj, comp, len(vals)).Float64(nil)
	require.NoError(t, err)

	return desc.Codec, got
}

// TestAutoCodecPicksDecimal asserts the adaptive path takes the denser scaled-decimal codec for
// the columns it compresses well — integer-valued (counter) and clean low-precision (gauge) —
// and that the values round-trip exactly.
func TestAutoCodecPicksDecimal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vals []float64
	}{
		{"counter-ints", []float64{100, 110, 121, 133, 146, 160}},
		{"one-decimal-gauge", []float64{604.5, 604.0, 606.3, 605.9, 607.1}},
		{"two-decimal-gauge", []float64{42.30, 42.31, 42.29, 42.33, 42.30}},
		{"negatives", []float64{-1.5, -2.0, -2.5, -1.0, -0.5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			codec, got := buildAuto(t, tc.vals, noneComp())
			assert.Equal(t, chunk.CodecDecimal, codec, "should pick the denser decimal codec")
			assert.Equal(t, tc.vals, got, "decimal must round-trip exactly")
		})
	}
}

// TestAutoCodecKeepsGorilla asserts the adaptive path keeps Gorilla for high-entropy columns
// the decimal codec cannot beat losslessly, and round-trips them exactly.
func TestAutoCodecKeepsGorilla(t *testing.T) {
	t.Parallel()

	// Full-precision irrationals (≈16 significant digits) exceed the decimal codec's 1e12
	// scaling, so it cannot reproduce them and Gorilla must win — and still round-trip exactly.
	vals := []float64{math.Pi, math.E, math.Sqrt2, math.SqrtPi, math.Phi, math.Ln2}

	codec, got := buildAuto(t, vals, noneComp())
	assert.Equal(t, chunk.CodecGorilla, codec, "high-entropy floats keep Gorilla")
	assert.Equal(t, vals, got)
}

// TestAutoCodecLosslessEdgeCases guards the adaptive choice against the float edge cases: NaN
// and ±Inf force Gorilla (the decimal codec cannot represent them), and a negative zero is
// value-preserved (it may normalize to +0, which is numerically identical for a metric).
func TestAutoCodecLosslessEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nan-keeps-gorilla", func(t *testing.T) {
		t.Parallel()

		vals := []float64{1, 2, math.NaN(), 4, 5}
		codec, got := buildAuto(t, vals, noneComp())
		assert.Equal(t, chunk.CodecGorilla, codec)
		require.Len(t, got, len(vals))
		assert.True(t, math.IsNaN(got[2]))
		assert.Equal(t, []float64{1, 2, 4, 5}, []float64{got[0], got[1], got[3], got[4]})
	})

	t.Run("inf-keeps-gorilla", func(t *testing.T) {
		t.Parallel()

		vals := []float64{1, math.Inf(1), 3, math.Inf(-1), 5}
		codec, got := buildAuto(t, vals, noneComp())
		assert.Equal(t, chunk.CodecGorilla, codec)
		assert.Equal(t, vals, got)
	})

	t.Run("negative-zero-among-decimals", func(t *testing.T) {
		t.Parallel()

		// A spurious -0 must not poison the column's choice: decimal still wins and every value
		// is numerically preserved (the -0 may come back as +0, which is == for a metric).
		vals := []float64{0.5, 1.0, math.Copysign(0, -1), 2.5, 1.5}
		codec, got := buildAuto(t, vals, noneComp())
		assert.Equal(t, chunk.CodecDecimal, codec)
		require.Len(t, got, len(vals))
		for i := range vals {
			assert.InDeltaf(t, vals[i], got[i], 0, "vals[%d] numerically preserved", i) // -0 == +0
		}
	})
}

// TestAutoCodecConstantCollapses confirms a constant float column still collapses to its manifest
// value (the const fast path runs before adaptive selection), costing no data object.
func TestAutoCodecConstantCollapses(t *testing.T) {
	t.Parallel()

	desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: makeConstFloats(64, 7.5), AutoCodec: true}, noneComp(), defaultGranuleSize)
	require.NoError(t, err)
	assert.True(t, desc.Const)
	assert.Nil(t, obj)
	assert.InDelta(t, 7.5, desc.ConstFloat64, 0)
}

func makeConstFloats(n int, v float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}

	return out
}

// TestAutoCodecLossyBudget covers the lossy regime: a precision budget on a high-entropy column
// takes the scaled-decimal path, encodes smaller than lossless, perturbs values within a bound,
// and records the budget in the descriptor (the merge engine's fixed point).
func TestAutoCodecLossyBudget(t *testing.T) {
	t.Parallel()

	vals := make([]float64, 256)
	for i := range vals {
		vals[i] = math.Sqrt(float64(i)+0.123456789) * 1000 // full-precision, high-entropy
	}

	descLossless, objLossless, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, AutoCodec: true}, noneComp(), defaultGranuleSize)
	require.NoError(t, err)
	assert.Equal(t, uint8(0), descLossless.FloatPrecisionBits)

	descLossy, objLossy, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, AutoCodec: true, FloatPrecisionBits: 12}, noneComp(), defaultGranuleSize)
	require.NoError(t, err)
	assert.Equal(t, uint8(12), descLossy.FloatPrecisionBits, "budget recorded for the fixed point")
	assert.Equal(t, chunk.CodecDecimal, descLossy.Codec, "lossy decimal wins on high-entropy data")
	assert.Less(t, len(objLossy), len(objLossless), "lossy encoding is smaller")

	got, err := newColumnReader(descLossy, objLossy, noneComp(), len(vals)).Float64(nil)
	require.NoError(t, err)

	changed := 0
	for i := range vals {
		assert.InEpsilonf(t, vals[i], got[i], 5e-2, "value[%d] within lossy bound (nearest-delta error accumulates along the series)", i)

		if got[i] != vals[i] {
			changed++
		}
	}
	assert.Positive(t, changed, "lossy budget must perturb some values")
}

// TestAutoCodecLossyNonFiniteFallsBack confirms a lossy budget on a column with NaN/Inf keeps the
// lossless Gorilla codec (decimal cannot represent non-finite values) while still recording the
// requested budget, so the merge engine does not loop trying to re-coarsen it.
func TestAutoCodecLossyNonFiniteFallsBack(t *testing.T) {
	t.Parallel()

	vals := []float64{1.5, math.NaN(), 2.5, math.Inf(1), 3.5}
	desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, AutoCodec: true, FloatPrecisionBits: 12}, noneComp(), defaultGranuleSize)
	require.NoError(t, err)
	assert.Equal(t, chunk.CodecGorilla, desc.Codec, "non-finite ⇒ lossless Gorilla")
	assert.Equal(t, uint8(12), desc.FloatPrecisionBits, "budget still recorded (fixed point)")

	got, err := newColumnReader(desc, obj, noneComp(), len(vals)).Float64(nil)
	require.NoError(t, err)
	assert.True(t, math.IsNaN(got[1]) && math.IsInf(got[3], 1))
	assert.Equal(t, []float64{1.5, 2.5, 3.5}, []float64{got[0], got[2], got[4]})
}

// TestManifestPrecisionRoundTrip confirms the lossy precision budget survives manifest
// encode/decode (a flag-gated byte), and that a lossless column adds no byte.
func TestManifestPrecisionRoundTrip(t *testing.T) {
	t.Parallel()

	m := Manifest{
		Version: 1, RowCount: 4, MinTime: 1, MaxTime: 9, GranuleSize: 8192,
		Columns: []ColumnDesc{
			{Name: "value", Kind: KindFloat64, Codec: chunk.CodecDecimal, FloatPrecisionBits: 16},
			{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD}, // lossless: no precision byte
		},
	}

	got, err := DecodeManifest(m.Encode(nil))
	require.NoError(t, err)
	require.Len(t, got.Columns, 2)
	assert.Equal(t, uint8(16), got.Columns[0].FloatPrecisionBits, "lossy budget persisted")
	assert.Equal(t, uint8(0), got.Columns[1].FloatPrecisionBits, "lossless column has no budget")
}

// FuzzAutoCodecLossless fuzzes the invariant the adaptive float codec must never violate:
// whichever codec it picks, the column round-trips with no loss of metric value — numeric
// equality, with NaN matching NaN. (A −0 may surface as +0; both branches of the comparison
// treat them as equal, which is correct for a metric value.) The seed bytes are reinterpreted
// as a float64 slice.
func FuzzAutoCodecLossless(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0x45, 0x40, 0, 0, 0, 0, 0, 0, 0x46, 0x40}) // {42, 44}
	f.Add([]byte{0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3f})             // {0.1}

	f.Fuzz(func(t *testing.T, seed []byte) {
		n := len(seed) / 8
		if n == 0 {
			t.Skip("no values")
		}

		vals := make([]float64, n)
		for i := range vals {
			bits := uint64(0)
			for b := range 8 {
				bits |= uint64(seed[i*8+b]) << (8 * b)
			}

			vals[i] = math.Float64frombits(bits)
		}

		for _, comp := range []*compress.Compressor{noneComp(), zstdComp()} {
			desc, obj, err := buildColumn(Column{Name: "v", Kind: KindFloat64, Float64: vals, AutoCodec: true}, comp, defaultGranuleSize)
			if err != nil {
				t.Fatalf("buildColumn: %v", err)
			}

			got, err := newColumnReader(desc, obj, comp, len(vals)).Float64(nil)
			if err != nil {
				t.Fatalf("decode (%s): %v", desc.Codec, err)
			}

			require.Len(t, got, len(vals))

			for i := range vals {
				if math.IsNaN(vals[i]) {
					if !math.IsNaN(got[i]) {
						t.Fatalf("codec %s vals[%d]: NaN became %v", desc.Codec, i, got[i])
					}

					continue
				}

				if got[i] != vals[i] {
					t.Fatalf("codec %s vals[%d]: %v != %v (bits %016x vs %016x)",
						desc.Codec, i, got[i], vals[i], math.Float64bits(got[i]), math.Float64bits(vals[i]))
				}
			}
		}
	})
}

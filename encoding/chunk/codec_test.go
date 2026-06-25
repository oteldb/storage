package chunk

import (
	"math"
	"testing"
)

func TestCodecString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		c    Codec
		want string
	}{
		{CodecNone, "none"},
		{CodecDoD, "dod"},
		{CodecGorilla, "gorilla"},
		{CodecDict, "dict"},
		{CodecT64, "t64"},
		{CodecDecimal, "decimal"},
		{Codec(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("Codec(%d).String() = %q, want %q", tc.c, got, tc.want)
		}
	}
}

func TestDoDAllCases(t *testing.T) {
	t.Parallel()
	// Exercise each DoD bit-width case explicitly.
	ts := []int64{
		0,                                    // row 0
		1000,                                 // row 1 (first delta = 1000)
		1000,                                 // dod = 0 → case 0b0
		1000 + 5000,                          // dod = 5000 → fits in 14 bits → case 0b10
		1000 + 5000 + 60000,                  // dod = 55000 → fits in 17 bits → case 0b110
		1000 + 5000 + 60000 + 500000,         // dod = 500000 → fits in 20 bits → case 0b1110
		1000 + 5000 + 60000 + 500000 + 1<<40, // dod = 2^40 → escape → case 0b1111
	}
	enc := EncodeTimestamps(nil, ts)
	got, _, err := DecodeTimestamps(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range ts {
		if got[i] != ts[i] {
			t.Fatalf("ts[%d] = %d, want %d", i, got[i], ts[i])
		}
	}
}

func TestDoDNegativeCases(t *testing.T) {
	t.Parallel()
	// Negative dod values in each bit-width case.
	ts := []int64{
		0,                            // row 0
		1000,                         // row 1
		1000,                         // dod = 0
		1000 - 5000,                  // dod = -5000 → 14 bits
		1000 - 5000 - 60000,          // dod = -60000 → 17 bits
		1000 - 5000 - 60000 - 500000, // dod = -500000 → 20 bits
	}
	enc := EncodeTimestamps(nil, ts)
	got, _, err := DecodeTimestamps(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range ts {
		if got[i] != ts[i] {
			t.Fatalf("ts[%d] = %d, want %d", i, got[i], ts[i])
		}
	}
}

func TestNearestDelta(t *testing.T) {
	t.Parallel()
	// With precisionBits=64, nearestDelta returns d unchanged.
	d, tz := nearestDelta(100, 200, 64, 0)
	if d != 100 || tz != 0 {
		t.Errorf("precisionBits=64: d=%d tz=%d, want d=100 tz=0", d, tz)
	}

	// With precisionBits=8, a large origin zeros trailing bits.
	// Use prevTZ=10 so the counter-reset hysteresis (±4) doesn't fire.
	d, tz = nearestDelta(12345, 1000000, 8, 10)
	if tz == 0 {
		t.Errorf("precisionBits=8: expected tz > 0, got %d", tz)
	}
	// The zeroed delta should have low bits cleared.
	if d != 0 && d&((1<<tz)-1) != 0 {
		t.Errorf("zeroed delta has low bits set: d=%d tz=%d", d, tz)
	}

	// d == 0 fast path.
	d, tz = nearestDelta(0, 100, 16, 5)
	if d != 0 || tz != 5 {
		t.Errorf("d=0: got d=%d tz=%d, want d=0 tz=5", d, tz)
	}

	// Counter-reset hysteresis: sudden tz jump.
	d, tz = nearestDelta(100, 1<<60, 16, 0)
	if d != 100 {
		t.Errorf("counter reset (jump up): d=%d, want 100 (full precision)", d)
	}
	// Counter-reset hysteresis: sudden tz drop.
	d, tz = nearestDelta(100, 100, 16, 30)
	if d != 100 {
		t.Errorf("counter reset (jump down): d=%d, want 100 (full precision)", d)
	}
}

func TestFloatToDecimalFractional(t *testing.T) {
	t.Parallel()
	// Fractional value that exercises the slow path.
	v, e := floatToDecimal(1.5)
	if v != 15 || e != -1 {
		t.Errorf("floatToDecimal(1.5) = (%d, %d), want (15, -1)", v, e)
	}
	v, e = floatToDecimal(0.5)
	if v != 5 || e != -1 {
		t.Errorf("floatToDecimal(0.5) = (%d, %d), want (5, -1)", v, e)
	}
}

func TestErrorStrings(t *testing.T) {
	t.Parallel()
	if errEOF.Error() != "chunk: unexpected end of stream" {
		t.Errorf("errEOF.Error() = %q", errEOF.Error())
	}
	if errUnexpectedEOF.Error() != "chunk: truncated stream" {
		t.Errorf("errUnexpectedEOF.Error() = %q", errUnexpectedEOF.Error())
	}
}

func TestIsEOF(t *testing.T) {
	t.Parallel()
	if !IsEOF(errEOF) {
		t.Error("IsEOF(errEOF) = false, want true")
	}
	if IsEOF(errUnexpectedEOF) {
		t.Error("IsEOF(errUnexpectedEOF) = true, want false")
	}
}

func TestGorillaReuseCase(t *testing.T) {
	t.Parallel()
	// Force the "reuse" case: values where consecutive XORs have leading/trailing
	// within the previous window.
	vals := []float64{
		1.0, 1.0, 1.0, // delta=0 for second → unchanged case
		2.0, 2.0, 2.0, // XOR then unchanged
		3.0, 3.0, 3.0, // XOR then unchanged
	}
	enc := EncodeFloats(nil, vals)
	got, _, err := DecodeFloats(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range vals {
		if got[i] != vals[i] {
			t.Fatalf("vals[%d] = %v, want %v", i, got[i], vals[i])
		}
	}
}

func TestGorillaNaNRoundTrip(t *testing.T) {
	t.Parallel()
	vals := []float64{1.0, math.NaN(), 2.0, math.NaN()}
	enc := EncodeFloats(nil, vals)
	got, _, err := DecodeFloats(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got[0] != 1.0 || got[2] != 2.0 {
		t.Fatalf("non-NaN values wrong: %v %v", got[0], got[2])
	}
}

func TestGorillaAllBitsChanged(t *testing.T) {
	t.Parallel()
	// XOR with no leading/trailing zeros → sigbits=64 → sentinel case.
	vals := []float64{0.0, math.Float64frombits(0x8000000000000001)} // -5e-324 (denorm)
	enc := EncodeFloats(nil, vals)
	got, _, err := DecodeFloats(nil, enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if math.Float64bits(got[0]) != math.Float64bits(vals[0]) ||
		math.Float64bits(got[1]) != math.Float64bits(vals[1]) {
		t.Fatalf("vals = %#x %#x, want %#x %#x",
			math.Float64bits(got[0]), math.Float64bits(got[1]),
			math.Float64bits(vals[0]), math.Float64bits(vals[1]))
	}
}

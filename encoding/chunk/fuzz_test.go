package chunk

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// FuzzDoDRoundTrip fuzzes the DoD timestamp round-trip.
func FuzzDoDRoundTrip(f *testing.F) {
	f.Add([]byte{0x02, 0xd0, 0x8f, 0xe0, 0x93, 0x4d, 0xe8, 0xb8, 0x03}) // 2 samples

	f.Fuzz(func(t *testing.T, seed []byte) {
		// Interpret the seed as a sequence of int64s (varint-decoded) to build a
		// timestamp array, then round-trip it.
		vals := decodeSeedToInt64s(seed, 256)
		if len(vals) == 0 {
			t.Skip("no values")
		}
		// Make timestamps monotonic-ish for meaningful DoD compression.
		ts := make([]int64, len(vals))

		ts[0] = vals[0]
		for i := 1; i < len(vals); i++ {
			ts[i] = ts[i-1] + vals[i]
		}

		enc := EncodeTimestamps(nil, ts)

		got, _, err := DecodeTimestamps(nil, enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if len(got) != len(ts) {
			t.Fatalf("len = %d, want %d", len(got), len(ts))
		}

		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("ts[%d] = %d, want %d", i, got[i], ts[i])
			}
		}
	})
}

// FuzzGorillaRoundTrip fuzzes the Gorilla XOR float round-trip.
func FuzzGorillaRoundTrip(f *testing.F) {
	f.Add([]byte{0x02, 0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x45, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, seed []byte) {
		vals := decodeSeedToFloats(seed, 256)
		if len(vals) == 0 {
			t.Skip("no values")
		}

		enc := EncodeFloats(nil, vals)

		got, _, err := DecodeFloats(nil, enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if len(got) != len(vals) {
			t.Fatalf("len = %d, want %d", len(got), len(vals))
		}

		for i := range vals {
			// NaN is checked via IsNaN (NaN != NaN).
			if isNaN(vals[i]) != isNaN(got[i]) {
				t.Fatalf("vals[%d] NaN mismatch: %v vs %v", i, vals[i], got[i])
			}

			if !isNaN(vals[i]) && got[i] != vals[i] {
				t.Fatalf("vals[%d] = %v, want %v", i, got[i], vals[i])
			}
		}
	})
}

// FuzzT64RoundTrip fuzzes the T64 int64 round-trip.
func FuzzT64RoundTrip(f *testing.F) {
	f.Add([]byte{0x08, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}) // 8 values

	f.Fuzz(func(t *testing.T, seed []byte) {
		vals := decodeSeedToInt64s(seed, 256)
		if len(vals) == 0 {
			t.Skip("no values")
		}

		enc := EncodeIntsT64(nil, vals)

		got, _, err := DecodeIntsT64(nil, enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if len(got) != len(vals) {
			t.Fatalf("len = %d, want %d", len(got), len(vals))
		}

		for i := range vals {
			if got[i] != vals[i] {
				t.Fatalf("vals[%d] = %d, want %d", i, got[i], vals[i])
			}
		}
	})
}

// FuzzDictRoundTrip fuzzes the dictionary string round-trip.
func FuzzDictRoundTrip(f *testing.F) {
	f.Add([]byte("hello world hello world hello"))

	f.Fuzz(func(t *testing.T, seed []byte) {
		vals := decodeSeedToStrings(seed, 64, 16)
		if len(vals) == 0 {
			t.Skip("no values")
		}

		enc := EncodeBytes(nil, vals)

		got, _, err := DecodeBytes(nil, enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if len(got) != len(vals) {
			t.Fatalf("len = %d, want %d", len(got), len(vals))
		}

		for i := range vals {
			if !bytes.Equal(got[i], vals[i]) {
				t.Fatalf("vals[%d] = %q, want %q", i, got[i], vals[i])
			}
		}

		// The reusable DictEncoder must produce byte-identical output, and the split
		// DecodeBytesDict form must agree with the gather-form DecodeBytes.
		encReuse := NewDictEncoder()
		defer encReuse.Release()

		if got := encReuse.Encode(nil, vals); !bytes.Equal(got, enc) {
			t.Fatalf("DictEncoder output differs from EncodeBytes")
		}

		col, consumed, err := DecodeBytesDict(enc)
		if err != nil {
			t.Fatalf("DecodeBytesDict: %v", err)
		}

		if consumed != len(enc) {
			t.Fatalf("DecodeBytesDict consumed = %d, want %d", consumed, len(enc))
		}

		if col.Len() != len(vals) {
			t.Fatalf("DecodeBytesDict Len = %d, want %d", col.Len(), len(vals))
		}

		for i := range vals {
			if !bytes.Equal(col.At(i), vals[i]) {
				t.Fatalf("DecodeBytesDict At(%d) = %q, want %q", i, col.At(i), vals[i])
			}
		}
	})
}

// FuzzBytesRawRoundTrip fuzzes the raw bytes codec round-trip, exercising both the fixed-width
// path (uniform-length values) and the length-prefixed fallback (mixed widths), and confirming the
// gather-form DecodeBytes and the split-form DecodeBytesDict agree.
func FuzzBytesRawRoundTrip(f *testing.F) {
	f.Add([]byte("abcd\x00efgh\x00ijkl"), uint8(4)) // uniform width ⇒ fixed path
	f.Add([]byte("a\x00bb\x00ccc"), uint8(0))       // mixed widths ⇒ flat fallback

	f.Fuzz(func(t *testing.T, seed []byte, pad uint8) {
		vals := decodeSeedToStrings(seed, 64, 16)
		if len(vals) == 0 {
			t.Skip("no values")
		}

		// Optionally normalize every value to one width so the fixed-width path is taken.
		if w := int(pad) % 17; w > 0 {
			norm := make([][]byte, len(vals))
			for i, v := range vals {
				b := make([]byte, w)
				copy(b, v)
				norm[i] = b
			}

			vals = norm
		}

		enc := EncodeBytesRaw(nil, vals)

		got, consumed, err := DecodeBytes(nil, enc)
		if err != nil {
			t.Fatalf("DecodeBytes: %v", err)
		}

		if consumed != len(enc) {
			t.Fatalf("DecodeBytes consumed = %d, want %d", consumed, len(enc))
		}

		if len(got) != len(vals) {
			t.Fatalf("len = %d, want %d", len(got), len(vals))
		}

		for i := range vals {
			if !bytes.Equal(got[i], vals[i]) {
				t.Fatalf("vals[%d] = %q, want %q", i, got[i], vals[i])
			}
		}

		col, _, err := DecodeBytesDict(enc)
		if err != nil {
			t.Fatalf("DecodeBytesDict: %v", err)
		}

		if col.Len() != len(vals) {
			t.Fatalf("DecodeBytesDict Len = %d, want %d", col.Len(), len(vals))
		}

		for i := range vals {
			if !bytes.Equal(col.At(i), vals[i]) {
				t.Fatalf("DecodeBytesDict At(%d) = %q, want %q", i, col.At(i), vals[i])
			}
		}
	})
}

// FuzzDecodeBytesArbitrary feeds arbitrary bytes to both bytes-column decoders: they must report an
// error or decode cleanly, never panic — covering corrupt flag bytes and out-of-range fixed widths.
func FuzzDecodeBytesArbitrary(f *testing.F) {
	f.Add([]byte{0x03, 0x02, 0xff, 0xff, 0xff, 0xff}) // flagFixed (0x02) with a huge width
	f.Add([]byte{0x02, 0x02, 0x08})                   // flagFixed, width 8, but no value bytes

	f.Fuzz(func(_ *testing.T, src []byte) {
		_, _, _ = DecodeBytes(nil, src)

		if col, _, err := DecodeBytesDict(src); err == nil && col != nil {
			for i := range col.Len() {
				_ = col.At(i)
			}
		}
	})
}

// decodeSeedToInt64s reads a sequence of zigzag varints from the seed.
func decodeSeedToInt64s(seed []byte, maxVals int) []int64 {
	var vals []int64

	off := 0
	for off < len(seed) && len(vals) < maxVals {
		v, n := binary.Varint(seed[off:])
		if n <= 0 {
			break
		}

		vals = append(vals, v)
		off += n
	}

	return vals
}

// decodeSeedToFloats interprets 8-byte chunks of the seed as float64 bits.
func decodeSeedToFloats(seed []byte, maxVals int) []float64 {
	var vals []float64
	for i := 0; i+8 <= len(seed) && len(vals) < maxVals; i += 8 {
		vals = append(vals, math.Float64frombits(binary.LittleEndian.Uint64(seed[i:])))
	}

	return vals
}

// decodeSeedToStrings splits the seed on 0x00 bytes into bytes.
func decodeSeedToStrings(seed []byte, maxStrings, maxLen int) [][]byte {
	var vals [][]byte

	start := 0

	for i := 0; i <= len(seed) && len(vals) < maxStrings; i++ {
		if i == len(seed) || seed[i] == 0 {
			s := seed[start:i]
			if len(s) > maxLen {
				s = s[:maxLen]
			}

			if len(s) > 0 {
				vals = append(vals, s)
			}

			start = i + 1
		}
	}

	return vals
}

func isNaN(f float64) bool { return f != f }

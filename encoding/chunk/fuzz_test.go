package chunk

import (
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
		enc := EncodeStrings(nil, vals)
		got, _, err := DecodeStrings(nil, enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if len(got) != len(vals) {
			t.Fatalf("len = %d, want %d", len(got), len(vals))
		}
		for i := range vals {
			if got[i] != vals[i] {
				t.Fatalf("vals[%d] = %q, want %q", i, got[i], vals[i])
			}
		}
	})
}

// decodeSeedToInt64s reads a sequence of zigzag varints from the seed.
func decodeSeedToInt64s(seed []byte, max int) []int64 {
	var vals []int64
	off := 0
	for off < len(seed) && len(vals) < max {
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
func decodeSeedToFloats(seed []byte, max int) []float64 {
	var vals []float64
	for i := 0; i+8 <= len(seed) && len(vals) < max; i += 8 {
		vals = append(vals, math.Float64frombits(binary.LittleEndian.Uint64(seed[i:])))
	}
	return vals
}

// decodeSeedToStrings splits the seed on 0x00 bytes into strings.
func decodeSeedToStrings(seed []byte, maxStrings, maxLen int) []string {
	var vals []string
	start := 0
	for i := 0; i <= len(seed) && len(vals) < maxStrings; i++ {
		if i == len(seed) || seed[i] == 0 {
			s := seed[start:i]
			if len(s) > maxLen {
				s = s[:maxLen]
			}
			if len(s) > 0 {
				vals = append(vals, string(s))
			}
			start = i + 1
		}
	}
	return vals
}

func isNaN(f float64) bool { return f != f }

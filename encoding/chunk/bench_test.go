package chunk

import (
	"math"
	"testing"
)

func BenchmarkDoDEncode(b *testing.B) {
	ts := makeConstantStride(1000, 1_000_000_000, 15_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeTimestamps(nil, ts)
	}
}

func BenchmarkDoDDecode(b *testing.B) {
	ts := makeConstantStride(1000, 1_000_000_000, 15_000)
	enc := EncodeTimestamps(nil, ts)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeTimestamps(nil, enc)
	}
}

func BenchmarkDoDEncodeJittered(b *testing.B) {
	ts := makeJittered(1000, 1_000_000_000, 15_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeTimestamps(nil, ts)
	}
}

func BenchmarkGorillaEncode(b *testing.B) {
	vals := makeSlowFloats(1000, 42.0, 0.001)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeFloats(nil, vals)
	}
}

func BenchmarkGorillaDecode(b *testing.B) {
	vals := makeSlowFloats(1000, 42.0, 0.001)
	enc := EncodeFloats(nil, vals)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeFloats(nil, enc)
	}
}

func BenchmarkGorillaEncodeConstant(b *testing.B) {
	vals := makeConstantFloats(1000, 42.0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeFloats(nil, vals)
	}
}

func BenchmarkT64Encode(b *testing.B) {
	vals := makeRange(0, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIntsT64(nil, vals)
	}
}

func BenchmarkT64Decode(b *testing.B) {
	vals := makeRange(0, 1000)
	enc := EncodeIntsT64(nil, vals)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeIntsT64(nil, enc)
	}
}

func BenchmarkDictEncode(b *testing.B) {
	vals := makeLowCardStrings(1000, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeStrings(nil, vals)
	}
}

func BenchmarkDictDecode(b *testing.B) {
	vals := makeLowCardStrings(1000, 10)
	enc := EncodeStrings(nil, vals)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeStrings(nil, enc)
	}
}

func BenchmarkDecimalEncode(b *testing.B) {
	vals := make([]float64, 1000)
	for i := range vals {
		vals[i] = math.Round(float64(i)*100) / 100
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeFloatsDecimal(nil, vals, 64)
	}
}

func BenchmarkDecimalDecode(b *testing.B) {
	vals := make([]float64, 1000)
	for i := range vals {
		vals[i] = math.Round(float64(i)*100) / 100
	}
	enc := EncodeFloatsDecimal(nil, vals, 64)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeFloatsDecimal(nil, enc)
	}
}

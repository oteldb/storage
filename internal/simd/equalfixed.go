package simd

import "bytes"

// EqualFixed16Width is the fixed row width [EqualFixed16] requires: both blob's rows and needle
// must be exactly this many bytes. It is a hardcoded property of the kernel (the AVX2 path
// compares a 16-byte needle broadcast into a 32-byte vector), not a parameter — a caller
// comparing against len(needle) or some other column's width instead of this constant is
// asserting the wrong thing, since neither actually guarantees the kernel's real precondition.
const EqualFixed16Width = 16

// equalFixed16Generic is the portable reference for [EqualFixed16]: it sets dst[i] = 1 where
// blob's i-th 16-byte row equals needle, else 0. len(blob) must be len(dst)*16 and len(needle)
// must be 16.
func equalFixed16Generic(blob, needle, dst []byte) {
	for i := range dst {
		row := blob[i*EqualFixed16Width : i*EqualFixed16Width+EqualFixed16Width]
		if bytes.Equal(row, needle) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

package simd

import "bytes"

// equalFixed16Generic is the portable reference for [EqualFixed16]: it sets dst[i] = 1 where
// blob's i-th 16-byte row equals needle, else 0. len(blob) must be len(dst)*16 and len(needle)
// must be 16.
func equalFixed16Generic(blob, needle, dst []byte) {
	for i := range dst {
		row := blob[i*16 : i*16+16]
		if bytes.Equal(row, needle) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

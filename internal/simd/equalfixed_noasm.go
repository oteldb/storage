//go:build !amd64

package simd

// EqualFixed16 sets dst[i] = 1 where the i-th 16-byte row of blob equals needle, else 0.
// len(blob) must be len(dst)*16 and len(needle) must be 16. On non-amd64 architectures it is the
// portable reference implementation.
func EqualFixed16(blob, needle, dst []byte) {
	equalFixed16Generic(blob, needle, dst)
}

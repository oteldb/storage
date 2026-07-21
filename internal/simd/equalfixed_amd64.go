//go:build amd64

package simd

import "golang.org/x/sys/cpu"

// equalFixed16AVX2 is the generated AVX2 kernel (minmax_amd64.s). The caller guarantees len(dst)
// is even and len(blob) == len(dst)*16.
//
//go:noescape
func equalFixed16AVX2(blob, needle, dst []byte)

// equalFixed16MinRows is the smallest row count at which dispatching to the AVX2 kernel amortizes
// its call/VZEROUPPER overhead; below it the generic loop is used regardless of CPU support.
const equalFixed16MinRows = 32

// EqualFixed16 sets dst[i] = 1 where the i-th 16-byte row of blob equals needle, else 0.
// len(blob) must be len(dst)*16 and len(needle) must be 16. On a CPU with AVX2 and enough rows to
// amortize dispatch it uses the vector kernel, else the portable reference; both agree bit-for-bit.
func EqualFixed16(blob, needle, dst []byte) {
	if len(dst) >= equalFixed16MinRows && cpu.X86.HasAVX2 {
		even := len(dst) &^ 1
		equalFixed16AVX2(blob[:even*16], needle, dst[:even])
		if even < len(dst) {
			equalFixed16Generic(blob[even*16:], needle, dst[even:])
		}
		return
	}

	equalFixed16Generic(blob, needle, dst)
}

//go:build amd64

package simd

import "golang.org/x/sys/cpu"

// minMaxInt64AVX2 is the generated AVX2 kernel (minmax_amd64.s). The caller guarantees len(s) >= 1.
//
//go:noescape
func minMaxInt64AVX2(s []int64) (rmin, rmax int64)

// MinMaxInt64 returns the minimum and maximum of s (and 0, 0 for an empty slice). On a CPU with
// AVX2 and a slice long enough to amortize the dispatch it uses the vector kernel, else the
// portable reference; both return identical results.
func MinMaxInt64(s []int64) (mn, mx int64) {
	if len(s) == 0 {
		return 0, 0
	}

	if len(s) >= 8 && cpu.X86.HasAVX2 {
		return minMaxInt64AVX2(s)
	}

	return minMaxInt64Generic(s)
}

// minMaxFloat64AVX2 is the generated AVX2 kernel (minmax_amd64.s).
//
//go:noescape
func minMaxFloat64AVX2(s []float64) (rmin, rmax float64)

// MinMaxFloat64 returns the minimum and maximum of s, ignoring NaN. An empty or all-NaN slice
// returns (+Inf, -Inf) — i.e. min > max — the sentinel a caller reads as "no real values". On a CPU
// with AVX2 and a slice long enough to amortize the dispatch it uses the vector kernel, else the
// portable reference; both return identical results.
func MinMaxFloat64(s []float64) (mn, mx float64) {
	if len(s) >= 8 && cpu.X86.HasAVX2 {
		return minMaxFloat64AVX2(s)
	}

	return minMaxFloat64Generic(s)
}

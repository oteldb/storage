//go:build !amd64

package simd

// MinMaxInt64 returns the minimum and maximum of s (and 0, 0 for an empty slice). On non-amd64
// architectures it is the portable reference implementation.
func MinMaxInt64(s []int64) (mn, mx int64) {
	if len(s) == 0 {
		return 0, 0
	}

	return minMaxInt64Generic(s)
}

// MinMaxFloat64 returns the minimum and maximum of s, ignoring NaN; an empty or all-NaN slice
// returns (+Inf, -Inf). On non-amd64 architectures it is the portable reference implementation.
func MinMaxFloat64(s []float64) (mn, mx float64) {
	return minMaxFloat64Generic(s)
}

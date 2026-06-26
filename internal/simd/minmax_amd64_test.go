//go:build amd64

package simd

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

// FuzzMinMaxInt64 pins the AVX2 kernel bit-for-bit against the pure-Go reference for arbitrary
// inputs — the contract that lets the library use whichever path the CPU supports.
func FuzzMinMaxInt64(f *testing.F) {
	if !cpu.X86.HasAVX2 {
		f.Skip("AVX2 not available")
	}

	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	f.Add(make([]byte, 8*10))

	f.Fuzz(func(t *testing.T, raw []byte) {
		n := len(raw) / 8
		if n == 0 {
			return
		}

		s := make([]int64, n)
		for i := range s {
			s[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
		}

		wmn, wmx := minMaxInt64Generic(s)
		gmn, gmx := minMaxInt64AVX2(s)
		require.Equal(t, wmn, gmn, "min mismatch for %v", s)
		require.Equal(t, wmx, gmx, "max mismatch for %v", s)
	})
}

func BenchmarkMinMaxInt64Generic(b *testing.B) {
	s := make([]int64, 4096)
	for i := range s {
		s[i] = int64(i*2654435761) ^ int64(i)
	}

	b.SetBytes(int64(len(s)) * 8)
	for b.Loop() {
		minMaxInt64Generic(s)
	}
}

func BenchmarkMinMaxInt64AVX2(b *testing.B) {
	if !cpu.X86.HasAVX2 {
		b.Skip("AVX2 not available")
	}

	s := make([]int64, 4096)
	for i := range s {
		s[i] = int64(i*2654435761) ^ int64(i)
	}

	b.SetBytes(int64(len(s)) * 8)
	for b.Loop() {
		minMaxInt64AVX2(s)
	}
}

//go:build amd64

package simd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

// FuzzEqualFixed16 pins the AVX2 kernel bit-for-bit against the pure-Go reference for arbitrary
// row counts and content, including the odd-tail-row split handled by the Go wrapper.
func FuzzEqualFixed16(f *testing.F) {
	if !cpu.X86.HasAVX2 {
		f.Skip("AVX2 not available")
	}

	f.Add(make([]byte, 16), make([]byte, 16*3), 3)
	f.Add(make([]byte, 16), make([]byte, 16*65), 65)

	f.Fuzz(func(t *testing.T, needle []byte, blob []byte, rows int) {
		if len(needle) != 16 || rows < 0 || rows > len(blob)/16 {
			return
		}
		blob = blob[:rows*16]

		want := make([]byte, rows)
		equalFixed16Generic(blob, needle, want)

		got := make([]byte, rows)
		EqualFixed16(blob, needle, got)

		require.Equal(t, want, got)
	})
}

// TestEqualFixed16Unaligned proves the AVX2 kernel is safe for the base-pointer alignments a real
// caller actually hands it: [bitstream.Reader.ReadBytesView] returns a view at an arbitrary byte
// offset into a larger decode buffer, not a fresh word/vector-aligned allocation. It builds blob
// and needle as sub-slices starting at every offset in [0,31] within a padded backing array (so
// every base address mod 32 is exercised) and checks AVX2 against the generic reference across row
// counts straddling the dispatch threshold (equalFixed16MinRows) and the odd-tail split.
func TestEqualFixed16Unaligned(t *testing.T) {
	t.Parallel()

	if !cpu.X86.HasAVX2 {
		t.Skip("AVX2 not available")
	}

	const maxRows = 40
	rowCases := []int{0, 1, equalFixed16MinRows - 1, equalFixed16MinRows, equalFixed16MinRows + 1, maxRows}

	for blobOff := range 32 {
		for needleOff := range 32 {
			for _, rows := range rowCases {
				name := fmt.Sprintf("blobOff=%d/needleOff=%d/rows=%d", blobOff, needleOff, rows)
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					backing := make([]byte, blobOff+rows*16+64)
					for i := range backing {
						backing[i] = byte(i * 2654435761)
					}
					blob := backing[blobOff : blobOff+rows*16]

					needleBacking := make([]byte, needleOff+16+64)
					for i := range needleBacking {
						needleBacking[i] = byte(i * 40503)
					}
					needle := needleBacking[needleOff : needleOff+16]

					if rows > 0 {
						copy(blob[(rows/2)*16:(rows/2+1)*16], needle) // seed a real match
					}

					want := make([]byte, rows)
					equalFixed16Generic(blob, needle, want)

					got := make([]byte, rows)
					EqualFixed16(blob, needle, got)

					assert.Equal(t, want, got)
				})
			}
		}
	}
}

package bitstream

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeReferenceBits(w *Writer, value uint64, width int) {
	for bit := width - 1; bit >= 0; bit-- {
		w.WriteBit(value&(uint64(1)<<uint(bit)) != 0)
	}
}

func TestWriteBitsMatchesReferenceAtEveryOffset(t *testing.T) {
	t.Parallel()
	for offset := range 8 {
		t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
			t.Parallel()
			for width := range 65 {
				for _, value := range []uint64{0, 1, ^uint64(0), 0x0123456789abcdef, 1 << 63} {
					for _, capacity := range []int{1, 8, 32} {
						got, want := NewWriter(make([]byte, 1, capacity)), NewWriter(nil)
						got.stream[0] = 0xa5
						require.NoError(t, want.WriteByte(0xa5))
						writeReferenceBits(got, 0x55, offset)
						writeReferenceBits(want, 0x55, offset)
						got.WriteBits(value, width)
						writeReferenceBits(want, value, width)
						got.WriteBits(5, 3)
						writeReferenceBits(want, 5, 3)
						require.Equal(t, want.Bytes(), got.Bytes(), "width=%d value=%x cap=%d", width, value, capacity)
						require.Equal(t, want.count, got.count)
					}
				}
			}
		})
	}
}

func FuzzWriteBitsReference(f *testing.F) {
	f.Add(uint8(64), uint8(0), uint64(0x123456789abcdef))
	f.Add(uint8(13), uint8(3), ^uint64(0))
	f.Fuzz(func(t *testing.T, width, offset uint8, value uint64) {
		width %= 65
		offset %= 8
		got, want := NewWriter(nil), NewWriter(nil)
		writeReferenceBits(got, 0x55, int(offset))
		writeReferenceBits(want, 0x55, int(offset))
		got.WriteBits(value, int(width))
		writeReferenceBits(want, value, int(width))
		got.WriteBits(5, 3)
		writeReferenceBits(want, 5, 3)
		require.Equal(t, want.Bytes(), got.Bytes())
		require.Equal(t, want.count, got.count)
	})
}

func BenchmarkWriteBitsWidths(b *testing.B) {
	for _, width := range []int{1, 8, 13, 32, 56, 64} {
		for _, offset := range []int{0, 3} {
			b.Run(fmt.Sprintf("width=%d/offset=%d", width, offset), func(b *testing.B) {
				w := NewWriter(make([]byte, 0, 4096))
				b.SetBytes(256 * 8)
				b.ReportAllocs()
				for b.Loop() {
					w.Reset(w.stream[:0])
					w.WriteBits(5, offset)
					for range 256 {
						w.WriteBits(0x0123456789abcdef, width)
					}
				}
			})
		}
	}
}

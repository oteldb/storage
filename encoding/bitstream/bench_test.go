package bitstream

import (
	"encoding/binary"
	"io"
	"testing"
)

// BenchmarkWriteBits measures the hot write path: 64-bit fields, full-byte aligned
// after the first. This is the inner loop of Gorilla/DoD encoders.
func BenchmarkWriteBits(b *testing.B) {
	b.ReportAllocs()
	w := NewWriter(make([]byte, 0, 1<<16))
	b.ResetTimer()
	for range b.N {
		w.Reset(w.stream[:0])
		for range 1024 {
			w.WriteBits(0x0123456789abcdef, 64)
		}
	}
}

// BenchmarkWriteBitsUnaligned measures 13-bit fields (typical Gorilla leading-zero
// count width), stressing the partial-byte path.
func BenchmarkWriteBitsUnaligned(b *testing.B) {
	b.ReportAllocs()
	w := NewWriter(make([]byte, 0, 1<<16))
	b.ResetTimer()
	for range b.N {
		w.Reset(w.stream[:0])
		for range 1024 {
			w.WriteBits(0x1ace, 13)
		}
	}
}

// BenchmarkReadBits measures the hot read path: 64-bit fields, mostly buffer hits.
func BenchmarkReadBits(b *testing.B) {
	w := NewWriter(make([]byte, 0, 1<<20))
	for range 8192 {
		w.WriteBits(0x0123456789abcdef, 64)
	}
	data := w.Bytes()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewReader(data)
		for range 8192 {
			_, _ = r.ReadBits(64)
		}
	}
}

// BenchmarkReadBitsUnaligned measures 13-bit reads (partial-byte span path).
func BenchmarkReadBitsUnaligned(b *testing.B) {
	w := NewWriter(make([]byte, 0, 1<<16))
	for range 4096 {
		w.WriteBits(0x1ace, 13)
	}
	data := w.Bytes()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewReader(data)
		for range 4096 {
			_, _ = r.ReadBits(13)
		}
	}
}

// BenchmarkReadBit measures single-bit reads (Gorilla control bits).
func BenchmarkReadBit(b *testing.B) {
	w := NewWriter(make([]byte, 0, 1<<16))
	for range 65536 {
		w.WriteBit(true)
	}
	data := w.Bytes()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewReader(data)
		for range 65536 {
			_, _ = r.ReadBit()
		}
	}
}

// BenchmarkVarintWrite measures the varint write path.
func BenchmarkVarintWrite(b *testing.B) {
	b.ReportAllocs()
	w := NewWriter(make([]byte, 0, 1<<16))
	vals := []int64{0, 1, -1, 1 << 20, -(1 << 20), 1 << 40, 1<<63 - 1}
	b.ResetTimer()
	for i := range b.N {
		w.Reset(w.stream[:0])
		for range 1024 {
			w.WriteVarint(vals[i%len(vals)])
		}
	}
}

// BenchmarkVarintRead measures the varint read path, compared against stdlib.
func BenchmarkVarintRead(b *testing.B) {
	w := NewWriter(make([]byte, 0, 1<<16))
	vals := []int64{0, 1, -1, 1 << 20, -(1 << 20), 1 << 40, 1<<63 - 1}
	const n = 1024
	for range n {
		w.WriteVarint(vals[len(vals)-1]) // worst-case width
	}
	data := w.Bytes()
	b.SetBytes(int64(len(data) / n))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewReader(data)
		for range n {
			_, _ = r.ReadVarint()
		}
	}
}

// BenchmarkStdlibVarintRead is a baseline: binary.ReadVarint over a bytes.Reader.
func BenchmarkStdlibVarintRead(b *testing.B) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, 1<<63-1)
	data := make([]byte, 0, n*1024)
	for range 1024 {
		data = append(data, buf[:n]...)
	}
	b.SetBytes(int64(n))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		br := bytesReader{data: data}
		for range 1024 {
			_, _ = binary.ReadVarint(&br)
		}
	}
}

// bytesReader is a minimal io.ByteReader to drive binary.ReadVarint without the
// bytes.Reader method-set overhead skewing the baseline.
type bytesReader struct {
	data []byte
	off  int
}

func (r *bytesReader) ReadByte() (byte, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	b := r.data[r.off]
	r.off++
	return b, nil
}

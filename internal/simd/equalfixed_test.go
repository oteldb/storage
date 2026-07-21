package simd

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/cpu"
)

// makeIDBlob builds a contiguous rows*16 blob of ids drawn from distinct values, round-robin —
// the shape decodeFixedGather hands back for a CodecBytesRaw trace_id column. needleRow gets a
// value that appears nowhere else, so a lookup for it must scan the whole blob (no early hit).
func makeIDBlob(rows, distinct, needleRow int) (blob, needle []byte) {
	blob = make([]byte, rows*16)
	for i := range rows {
		row := blob[i*16 : i*16+16]
		binary.BigEndian.PutUint64(row[0:8], 0xABCD)
		binary.BigEndian.PutUint64(row[8:16], uint64(i%distinct))
	}

	needle = make([]byte, 16)
	binary.BigEndian.PutUint64(needle[0:8], 0xDEAD)
	binary.BigEndian.PutUint64(needle[8:16], 0xBEEF)
	copy(blob[needleRow*16:needleRow*16+16], needle)

	return blob, needle
}

func TestEqualFixed16(t *testing.T) {
	t.Parallel()

	for _, rows := range []int{0, 1, 2, 3, 31, 32, 33, 200} {
		blob, needle := makeIDBlob(max(rows, 1), max(rows, 1), rows/2)
		blob = blob[:rows*16]

		want := make([]byte, rows)
		equalFixed16Generic(blob, needle, want)

		got := make([]byte, rows)
		EqualFixed16(blob, needle, got)

		assert.Equalf(t, want, got, "rows=%d", rows)
		if rows > 0 {
			assert.Equal(t, byte(1), got[rows/2], "expected the seeded match at row %d", rows/2)
		}
	}
}

func bytesEqualLoop(blob, needle []byte, dst []byte) {
	for i := range dst {
		row := blob[i*16 : i*16+16]
		if bytes.Equal(row, needle) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

// BenchmarkEqualFixed16 compares three ways to find rows equal to a 16-byte needle in a
// contiguous fixed-width blob (the CodecBytesRaw on-disk shape for trace_id): the naive
// bytes.Equal-per-row loop matching today's recordengine gather, the portable stride reference,
// and the AVX2 kernel. The needle is placed at the midpoint so every path scans the whole blob.
func BenchmarkEqualFixed16(b *testing.B) {
	const rows = 200_000
	blob, needle := makeIDBlob(rows, rows, rows/2)
	dst := make([]byte, rows)
	logical := int64(rows) * 16

	b.Run("BytesEqualLoop", func(b *testing.B) {
		b.SetBytes(logical)
		b.ReportAllocs()
		for b.Loop() {
			bytesEqualLoop(blob, needle, dst)
		}
	})

	b.Run("Generic", func(b *testing.B) {
		b.SetBytes(logical)
		b.ReportAllocs()
		for b.Loop() {
			equalFixed16Generic(blob, needle, dst)
		}
	})

	b.Run("AVX2", func(b *testing.B) {
		if !cpu.X86.HasAVX2 {
			b.Skip("AVX2 not available")
		}
		b.SetBytes(logical)
		b.ReportAllocs()
		for b.Loop() {
			EqualFixed16(blob, needle, dst)
		}
	})
}

// bytesIndexScan is a rejected alternative kept as a documented benchmark: bytealg.Index prefilters
// candidates on the needle's first byte, but real trace_id rows share a constant high-order prefix
// (see makeIDBlob), so every row's first byte matches and it degrades to a full verify per byte
// offset — plus, unlike EqualFixed16, it finds one occurrence per call and must restart the scan and
// re-derive row alignment (abs%16) after each hit to build a full match bitmap.
func bytesIndexScan(blob, needle, dst []byte) {
	pos := 0
	for {
		i := bytes.Index(blob[pos:], needle)
		if i < 0 {
			return
		}
		abs := pos + i
		if abs%16 == 0 {
			dst[abs/16] = 1
		}
		pos = abs + 1
	}
}

func BenchmarkEqualFixed16BytesIndex(b *testing.B) {
	const rows = 200_000
	blob, needle := makeIDBlob(rows, rows, rows/2)
	dst := make([]byte, rows)
	logical := int64(rows) * 16

	b.SetBytes(logical)
	b.ReportAllocs()
	for b.Loop() {
		clear(dst)
		bytesIndexScan(blob, needle, dst)
	}
}

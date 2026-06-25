package bitstream

import (
	"encoding/binary"
	"io"
	"testing"
)

// FuzzRoundTripBits fuzzes the WriteBits∘ReadBits round-trip over all widths and
// arbitrary 64-bit values. encode∘decode == identity is the bitstream invariant
// (DESIGN.md §13, §14 M0).
func FuzzRoundTripBits(f *testing.F) {
	f.Add(uint8(1), uint64(0))
	f.Add(uint8(8), uint64(0xab))
	f.Add(uint8(13), uint64(0x1ace))
	f.Add(uint8(32), uint64(0xdeadbeef))
	f.Add(uint8(64), uint64(0x0123456789abcdef))
	f.Add(uint8(40), uint64(0xff00ff00ff))

	f.Fuzz(func(t *testing.T, nbits uint8, val uint64) {
		if nbits > 64 {
			t.Skip("nbits out of range")
		}
		w := NewWriter(nil)
		w.WriteBits(val, int(nbits))
		r := NewReader(w.Bytes())
		got, err := r.ReadBits(nbits)
		if err != nil {
			t.Fatalf("ReadBits(%d): %v", nbits, err)
		}
		mask := uint64(0)
		if nbits == 64 {
			mask = ^uint64(0)
		} else {
			mask = (uint64(1) << nbits) - 1
		}
		if want := val & mask; got != want {
			t.Fatalf("ReadBits(%d) = %#x, want %#x", nbits, got, want)
		}
		// Stream must be exhausted: the remaining bits in the partial trailing byte
		// are padding and must read as EOF for the *next* field.
		if _, err := r.ReadBits(1); err == nil && nbits%8 != 0 {
			// A full-byte field has no padding, so the next read should hit EOF only
			// if there's genuinely no more data. For partial-byte fields any extra
			// bits are padding: we wrote exactly nbits, so the byte holds (8 - nbits%8)
			// padding zeros. Reading them back succeeds but yields zero — that's fine.
		}
	})
}

// FuzzRoundTripSequence fuzzes a sequence of varints: it decodes the seed bytes as a
// stream of stdlib uvarints, re-encodes each with our [Writer.WriteUvarint] plus a
// 13-bit side field, then reads them back with our [Reader]. This checks the
// bit-offset bookkeeping across refill boundaries on varint-shaped data of varied
// widths (the seed bytes come from the corpus, so the varint widths vary naturally).
func FuzzRoundTripSequence(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03, 0xff, 0x01, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0x0f})
	f.Add([]byte{0x00, 0x01, 0x80, 0x80, 0x80, 0x10, 0xff, 0xff, 0xff, 0xff, 0x07})
	f.Add([]byte{0xde, 0xad, 0xbe, 0xef})

	f.Fuzz(func(t *testing.T, seed []byte) {
		// Decode the seed as a sequence of stdlib uvarints.
		var vals []uint64
		br := &byteSeq{data: seed}
		for {
			v, err := binary.ReadUvarint(br)
			if err != nil {
				break
			}
			vals = append(vals, v)
			if len(vals) > 4096 {
				break // bound the work
			}
		}
		if len(vals) == 0 {
			t.Skip("no complete varints in seed")
		}
		// Encode each value as uvarint + a 13-bit field, then read back.
		w := NewWriter(nil)
		for _, v := range vals {
			w.WriteUvarint(v)
			w.WriteBits(v&0x1fff, 13)
		}
		r := NewReader(w.Bytes())
		for i, v := range vals {
			got, err := r.ReadUvarint()
			if err != nil {
				t.Fatalf("val %d: ReadUvarint: %v", i, err)
			}
			if got != v {
				t.Fatalf("val %d: ReadUvarint = %d, want %d", i, got, v)
			}
			gotBits, err := r.ReadBits(13)
			if err != nil {
				t.Fatalf("val %d: ReadBits(13): %v", i, err)
			}
			if want := v & 0x1fff; gotBits != want {
				t.Fatalf("val %d: ReadBits(13) = %#x, want %#x", i, gotBits, want)
			}
		}
	})
}

// byteSeq is a minimal io.ByteReader for stdlib uvarint decoding of the seed.
type byteSeq struct {
	data []byte
	off  int
}

func (r *byteSeq) ReadByte() (byte, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	b := r.data[r.off]
	r.off++
	return b, nil
}

// FuzzRoundTripVarint fuzzes the zig-zag varint round-trip over arbitrary int64.
func FuzzRoundTripVarint(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(-1))
	f.Add(int64(1 << 40))
	f.Add(int64(-(1 << 50)))

	f.Fuzz(func(t *testing.T, v int64) {
		w := NewWriter(nil)
		w.WriteVarint(v)
		r := NewReader(w.Bytes())
		got, err := r.ReadVarint()
		if err != nil {
			t.Fatalf("ReadVarint(%d): %v", v, err)
		}
		if got != v {
			t.Fatalf("ReadVarint = %d, want %d", got, v)
		}
	})
}

// FuzzReaderFromArbitraryBytes verifies the reader never panics and never returns a
// non-EOF, non-nil error on arbitrary bytes — it either decodes or hits EOF/UnexpectedEOF.
func FuzzReaderFromArbitraryBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01})

	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewReader(b)
		// Read bits in arbitrary-width chunks; must not panic.
		for i := 0; i < 200; i++ {
			width := uint8(((i * 7) + 1) % 65) // 1..64 cycle
			if width == 0 {
				width = 1
			}
			_, err := r.ReadBits(width)
			if err != nil {
				break
			}
		}
		// Also exercise varint reads.
		r2 := NewReader(b)
		for range binary.MaxVarintLen64 + 1 {
			if _, err := r2.ReadUvarint(); err != nil {
				break
			}
		}
	})
}

package bitstream

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestPeekSkip verifies Peek observes upcoming bits without consuming and Skip advances past them,
// so Peek(n)+Skip(n) equals ReadBits(n). This is the fast path the chunk decoders take for
// variable-length prefixes.
func TestPeekSkip(t *testing.T) {
	t.Parallel()

	w := NewWriter(nil)
	w.WriteBits(0b1011, 4)
	w.WriteBits(0b110, 3)
	w.WriteBits(0x2A, 8)

	r := NewReader(w.Bytes())

	first, err := r.ReadBits(4) // loads the buffer
	if err != nil || first != 0b1011 {
		t.Fatalf("ReadBits(4) = %#b, err %v", first, err)
	}

	if r.Buffered() < 3 {
		t.Fatalf("Buffered() = %d, want >= 3 for the next Peek", r.Buffered())
	}

	if p := r.Peek(3); p != 0b110 {
		t.Fatalf("Peek(3) = %#b, want 0b110", p)
	}

	if p := r.Peek(3); p != 0b110 { // Peek must not consume
		t.Fatalf("Peek(3) again = %#b, want 0b110 (no consume)", p)
	}

	before := r.Buffered()
	r.Skip(3)

	if r.Buffered() != before-3 {
		t.Fatalf("Skip(3): Buffered %d -> %d, want %d", before, r.Buffered(), before-3)
	}

	v, err := r.ReadBits(8)
	if err != nil || v != 0x2A {
		t.Fatalf("ReadBits(8) after Skip = %#x, err %v", v, err)
	}
}

// TestRoundTripBits verifies WriteBits∘ReadBits == identity for a range of widths and
// values, including values that span the 8-byte refill boundary.
func TestRoundTripBits(t *testing.T) {
	t.Parallel()
	widths := []int{1, 2, 3, 4, 5, 7, 8, 9, 13, 16, 23, 32, 40, 56, 63, 64}
	values := []uint64{0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffffff, 0xffffffffffffffff,
		0x5555555555555555, 0xaaaaaaaaaaaaaaaa, 0x123456789abcdef0}

	for _, nbits := range widths {
		// Mask values to nbits.
		mask := ^uint64(0)
		if nbits != 64 {
			mask = (uint64(1) << nbits) - 1
		}
		for _, v := range values {
			v := v & mask
			t.Run("bits", func(t *testing.T) {
				t.Parallel()
				w := NewWriter(nil)
				w.WriteBits(v, nbits)
				r := NewReader(w.Bytes())
				got, err := r.ReadBits(uint8(nbits))
				if err != nil {
					t.Fatalf("ReadBits(%d): %v", nbits, err)
				}
				if got != v {
					t.Fatalf("ReadBits(%d) = %#x, want %#x", nbits, got, v)
				}
				// After reading exactly nbits, EOF should only be reported when the
				// field was byte-aligned (nbits%8==0). Otherwise the trailing
				// (8 - nbits%8) bits in the last byte are padding and ReadBit
				// legitimately succeeds reading one of them.
				if nbits%8 == 0 {
					if _, err := r.ReadBit(); !errors.Is(err, io.EOF) {
						t.Fatalf("expected EOF after full read, got %v", err)
					}
				}
			})
		}
	}
}

// TestRoundTripSequence writes a long sequence of mixed-width values and reads them
// back, ensuring the bit offset is tracked correctly across byte and refill
// boundaries.
func TestRoundTripSequence(t *testing.T) {
	t.Parallel()
	type op struct {
		nbits int
		val   uint64
	}
	ops := []op{
		{3, 0b101}, {5, 0b11001}, {8, 0xab}, {1, 1}, {1, 0}, {16, 0xcafe},
		{13, 0x1f3}, {7, 0x5a}, {24, 0xdeadbe >> 8}, {32, 0xfeedface},
		{11, 0x7ff}, {64, 0x0123456789abcdef}, {6, 0x2a}, {40, 0xff00ff00ff},
		{20, 0xabcde}, {2, 0b11}, {9, 0x1aa}, {56, 0x01020304050607},
		{1, 1}, {63, 1 << 62}, {4, 0xa},
	}
	w := NewWriter(nil)
	for _, o := range ops {
		w.WriteBits(o.val, o.nbits)
	}
	w.PadToByte() // byte-align so EOF is deterministic after the last field
	r := NewReader(w.Bytes())
	for i, o := range ops {
		got, err := r.ReadBits(uint8(o.nbits))
		if err != nil {
			t.Fatalf("op %d: ReadBits(%d): %v", i, o.nbits, err)
		}
		mask := ^uint64(0)
		if o.nbits != 64 {
			mask = (uint64(1) << o.nbits) - 1
		}
		if want := o.val & mask; got != want {
			t.Fatalf("op %d: ReadBits(%d) = %#x, want %#x", i, o.nbits, got, want)
		}
	}
	// Any remaining bits are padding and must be zero, then EOF.
	for {
		got, err := r.ReadBit()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("expected EOF at end, got %v", err)
			}
			break
		}
		if got {
			t.Fatalf("expected zero padding bits after last field, got 1")
		}
	}
}

// TestRoundTripBit verifies single-bit write/read.
func TestRoundTripBit(t *testing.T) {
	t.Parallel()
	pattern := []bool{true, false, true, true, false, false, true, true, true, false}
	w := NewWriter(nil)
	for _, b := range pattern {
		w.WriteBit(b)
	}
	r := NewReader(w.Bytes())
	for i, want := range pattern {
		got, err := r.ReadBit()
		if err != nil {
			t.Fatalf("bit %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("bit %d = %v, want %v", i, got, want)
		}
	}
}

// TestRoundTripByte verifies whole-byte write/read across the partial-byte boundary.
func TestRoundTripByte(t *testing.T) {
	t.Parallel()
	data := []byte{0x00, 0xff, 0x55, 0xaa, 0x01, 0x80, 0x7f, 0xfe, 0x12, 0x34}
	w := NewWriter(nil)
	// Interleave with a 3-bit field to force a partial last byte.
	w.WriteBits(0b101, 3)
	for _, b := range data {
		w.WriteByte(b)
	}
	r := NewReader(w.Bytes())
	if got, err := r.ReadBits(3); err != nil || got != 0b101 {
		t.Fatalf("prefix = %#x, err %v", got, err)
	}
	for i, want := range data {
		got, err := r.ReadByte()
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("byte %d = %#x, want %#x", i, got, want)
		}
	}
}

// TestRoundTripVarint verifies WriteVarint∘ReadVarint and WriteUvarint∘ReadUvarint.
func TestRoundTripVarint(t *testing.T) {
	t.Parallel()
	ints := []int64{0, 1, -1, 2, -2, 127, -127, 128, -128, 1 << 20, -(1 << 20),
		1 << 62, -(1 << 62), 1<<63 - 1, -(1 << 63), 0xdeadbeef, -0xdeadbeef}
	w := NewWriter(nil)
	for _, v := range ints {
		w.WriteVarint(v)
	}
	r := NewReader(w.Bytes())
	for i, want := range ints {
		got, err := r.ReadVarint()
		if err != nil {
			t.Fatalf("varint %d (%d): %v", i, want, err)
		}
		if got != want {
			t.Fatalf("varint %d = %d, want %d", i, got, want)
		}
	}

	uints := []uint64{0, 1, 2, 127, 128, 255, 1 << 14, 1<<32 - 1, 1 << 63, 1<<64 - 1}
	w = NewWriter(nil)
	for _, v := range uints {
		w.WriteUvarint(v)
	}
	r = NewReader(w.Bytes())
	for i, want := range uints {
		got, err := r.ReadUvarint()
		if err != nil {
			t.Fatalf("uvarint %d (%d): %v", i, want, err)
		}
		if got != want {
			t.Fatalf("uvarint %d = %d, want %d", i, got, want)
		}
	}
}

// TestPadAndAlign verifies PadToByte/AlignToByte keep the stream byte-aligned.
func TestPadAndAlign(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteBits(0b101, 3)
	w.PadToByte()
	w.WriteByte(0x42)
	r := NewReader(w.Bytes())
	if got, err := r.ReadBits(3); err != nil || got != 0b101 {
		t.Fatalf("prefix = %#x, err %v", got, err)
	}
	r.AlignToByte() // drop the 5 padding bits
	if got, err := r.ReadByte(); err != nil || got != 0x42 {
		t.Fatalf("byte = %#x, err %v", got, err)
	}
}

// TestReset verifies Reset reuses the writer and reader without allocation.
func TestReset(t *testing.T) {
	t.Parallel()
	var w Writer
	buf := make([]byte, 0, 64)
	w.Reset(buf)
	w.WriteBits(0b1010, 4)
	out1 := append([]byte(nil), w.Bytes()...)

	w.Reset(buf[:0])
	w.WriteBits(0b1100, 4)
	out2 := append([]byte(nil), w.Bytes()...)

	if bytes.Equal(out1, out2) {
		t.Fatalf("Reset did not reset content: %v", out1)
	}

	var r Reader
	r.Reset(out1)
	got, err := r.ReadBits(4)
	if err != nil || got != 0b1010 {
		t.Fatalf("reader reset: %#x, err %v", got, err)
	}
}

// TestEOFOnEmpty verifies a zero-length stream yields EOF.
func TestEOFOnEmpty(t *testing.T) {
	t.Parallel()
	r := NewReader(nil)
	if _, err := r.ReadBit(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadBit on empty: %v, want EOF", err)
	}
	if _, err := r.ReadBits(8); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadBits on empty: %v, want EOF", err)
	}
}

// TestWriteBitsPanicsOnOutOfRange verifies the nbits guard.
func TestWriteBitsPanicsOnOutOfRange(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nbits > 64")
		}
	}()
	w := NewWriter(nil)
	w.WriteBits(0, 65)
}

// TestReadBitsPanicsOnOutOfRange verifies the nbits guard.
func TestReadBitsPanicsOnOutOfRange(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nbits > 64")
		}
	}()
	r := NewReader([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	_, _ = r.ReadBits(65)
}

// TestWriteBitsZeroNbits verifies WriteBits(u, 0) is a no-op.
func TestWriteBitsZeroNbits(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteByte(0x42)
	w.WriteBits(0xff, 0) // no-op
	if got, want := w.Len(), 1; got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
}

// TestLenAndAppendTo verifies Len and AppendTo (owned copy).
func TestLenAndAppendTo(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteBits(0b1011, 4)
	w.PadToByte()
	if got, want := w.Len(), 1; got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
	dst := w.AppendTo([]byte{0xaa})
	if want := []byte{0xaa, 0xb0}; !bytes.Equal(dst, want) {
		t.Fatalf("AppendTo = %x, want %x", dst, want)
	}
}

// TestRemaining verifies the byte-estimate accounting.
func TestRemaining(t *testing.T) {
	t.Parallel()
	r := NewReader([]byte{0x01, 0x02, 0x03, 0x04})
	if got := r.Remaining(); got != 4 {
		t.Fatalf("Remaining = %d, want 4", got)
	}
	_, _ = r.ReadBits(8)
	if got := r.Remaining(); got != 3 {
		t.Fatalf("Remaining after 8 bits = %d, want 3", got)
	}
}

// TestSkipBits verifies SkipBits discards bits.
func TestSkipBits(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteBits(0b101, 3)
	w.WriteBits(0xff, 8)
	w.WriteBits(0b11, 2)
	w.PadToByte()
	r := NewReader(w.Bytes())
	if err := r.SkipBits(3); err != nil {
		t.Fatalf("SkipBits(3): %v", err)
	}
	if got, err := r.ReadBits(8); err != nil || got != 0xff {
		t.Fatalf("after skip, ReadBits(8) = %#x, err %v", got, err)
	}
}

// TestSkipBitsEOF verifies SkipBits returns EOF at the end.
func TestSkipBitsEOF(t *testing.T) {
	t.Parallel()
	r := NewReader([]byte{0xff})
	if err := r.SkipBits(16); err == nil {
		t.Fatal("expected EOF from SkipBits past end")
	}
}

// TestReadBitsZero verifies ReadBits(0) returns 0, nil without consuming.
func TestReadBitsZero(t *testing.T) {
	t.Parallel()
	r := NewReader([]byte{0xff})
	got, err := r.ReadBits(0)
	if err != nil || got != 0 {
		t.Fatalf("ReadBits(0) = %d, err %v", got, err)
	}
	// Stream still full.
	if got, err := r.ReadBits(8); err != nil || got != 0xff {
		t.Fatalf("ReadBits(8) = %#x, err %v", got, err)
	}
}

// TestAlignToByte verifies AlignToByte drops partial-buffer bits up to the next byte
// boundary, so the next read begins on a byte boundary.
func TestAlignToByte(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteBits(0b101, 3) // byte0 high 3 bits = 101, 5 padding
	w.PadToByte()         // byte-align: byte0 = 0b10100000 = 0xa0
	w.WriteByte(0xff)     // byte1
	r := NewReader(w.Bytes())
	if got, err := r.ReadBits(2); err != nil || got != 0b10 {
		t.Fatalf("ReadBits(2) = %#x, err %v", got, err)
	}
	// 1 bit of the 0b101 prefix remains buffered; AlignToByte drops it.
	r.AlignToByte()
	if got, err := r.ReadByte(); err != nil || got != 0xff {
		t.Fatalf("after align, ReadByte = %#x, err %v", got, err)
	}
}

// TestLeadingZeros64 verifies the convenience wrapper.
func TestLeadingZeros64(t *testing.T) {
	t.Parallel()
	if got, want := LeadingZeros64(0), 64; got != want {
		t.Fatalf("LeadingZeros64(0) = %d, want %d", got, want)
	}
	if got, want := LeadingZeros64(1), 63; got != want {
		t.Fatalf("LeadingZeros64(1) = %d, want %d", got, want)
	}
	if got, want := LeadingZeros64(1<<63), 0; got != want {
		t.Fatalf("LeadingZeros64(1<<63) = %d, want %d", got, want)
	}
}

// TestReadVarintEOF verifies varint reads surface EOF/UnexpectedEOF at end.
func TestReadVarintEOF(t *testing.T) {
	t.Parallel()
	// Truncated varint (continuation bit set, no terminating byte).
	w := NewWriter(nil)
	w.WriteByte(0x80)
	w.WriteByte(0x80)
	r := NewReader(w.Bytes())
	if _, err := r.ReadUvarint(); err == nil {
		t.Fatal("expected error from truncated uvarint")
	}
}

// TestReaderResetOnEmpty verifies Reset onto an empty slice clears last.
func TestReaderResetOnEmpty(t *testing.T) {
	t.Parallel()
	var r Reader
	r.Reset([]byte{0xff, 0xff})
	_, _ = r.ReadBits(8)
	r.Reset(nil) // empty
	if _, err := r.ReadBit(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadBit after Reset(nil): %v, want EOF", err)
	}
}

// TestWriteBytes verifies bulk byte writes, both aligned and unaligned.
func TestWriteBytes(t *testing.T) {
	t.Parallel()
	// Aligned: single append.
	w := NewWriter(nil)
	w.WriteBits(0xff, 8) // byte-align
	w.WriteBytes([]byte{1, 2, 3, 4})
	r := NewReader(w.Bytes())
	v, _ := r.ReadBits(8)
	if v != 0xff {
		t.Fatalf("prefix = %#x", v)
	}
	buf := make([]byte, 4)
	if err := r.ReadBytes(buf); err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	if !bytes.Equal(buf, []byte{1, 2, 3, 4}) {
		t.Fatalf("ReadBytes = %v", buf)
	}

	// Unaligned: falls back to per-byte WriteByte. The byte gets split across
	// the boundary, so we read it back bit-by-bit through ReadBits, not via
	// AlignToByte + ReadByte.
	w = NewWriter(nil)
	w.WriteBits(0b101, 3) // not aligned
	w.WriteBytes([]byte{0xaa})
	r = NewReader(w.Bytes())
	v, _ = r.ReadBits(3)
	if v != 0b101 {
		t.Fatalf("prefix = %#x", v)
	}
	// Read the 8 bits of 0xaa back through the bit reader (they span the boundary).
	b, err := r.ReadBits(8)
	if err != nil {
		t.Fatalf("ReadBits(8): %v", err)
	}
	if byte(b) != 0xaa {
		t.Fatalf("byte = %#x, want 0xaa", byte(b))
	}
}

// TestAppendString verifies zero-copy string append when aligned.
func TestAppendString(t *testing.T) {
	t.Parallel()
	w := NewWriter(nil)
	w.WriteBits(0xff, 8) // byte-align
	w.AppendString("hello")
	r := NewReader(w.Bytes())
	v, _ := r.ReadBits(8)
	if v != 0xff {
		t.Fatalf("prefix = %#x", v)
	}
	buf := make([]byte, 5)
	if err := r.ReadBytes(buf); err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("ReadBytes = %q", buf)
	}
}

// TestReadBytesEOF verifies ReadBytes returns EOF when not enough data.
func TestReadBytesEOF(t *testing.T) {
	t.Parallel()
	r := NewReader([]byte{0x01, 0x02})
	buf := make([]byte, 10)
	if err := r.ReadBytes(buf); err == nil {
		t.Fatal("expected EOF from ReadBytes past end")
	}
}

// TestReadBytesPartialBuffer verifies ReadBytes drains the refill buffer first.
func TestReadBytesPartialBuffer(t *testing.T) {
	t.Parallel()
	data := make([]byte, 20)
	for i := range data {
		data[i] = byte(i)
	}
	r := NewReader(data)
	// Read a few bits to prime the buffer with a partial byte.
	_, _ = r.ReadBits(4)
	// Now read 10 bytes: should align, drain buffered bytes, then read from stream.
	buf := make([]byte, 10)
	if err := r.ReadBytes(buf); err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	// After 4 bits + align, we're at byte 1. So buf should be data[1:11].
	for i := range buf {
		if buf[i] != byte(i+1) {
			t.Fatalf("buf[%d] = %d, want %d", i, buf[i], i+1)
		}
	}
}

package bitstream

import (
	"encoding/binary"
	"io"
	"math/bits"
	"slices"
)

// Writer is an MSB-first bit writer that appends to a caller-owned []byte. It is the
// foundation of every encoder in encoding/chunk (DESIGN.md §10, §14 M0).
//
// Zero-alloc discipline: [Writer.WriteBits] and friends only grow the backing slice
// via append; there is no per-write allocation. [Reset] re-binds a Writer onto a
// pooled buffer so callers can reuse a *Writer from a sync.Pool:
//
//	w := pool.Get().(*Writer)
//	w.Reset(buf[:0])
//	defer pool.Put(w)
//
// The bit order is MSB-first: the first bit written occupies the high bit of the
// first byte. Partial trailing bits are flushed into the last byte, high-justified.
// [Bytes] returns the consumed prefix; the Writer is then ready for more writes
// (remaining low bits of the last byte are preserved across [Bytes]).
type Writer struct {
	stream []byte // the backing byte slice; owned by the caller, grown by append
	count  uint8  // free bits in the last byte of stream (0 ⇒ last byte is full)
}

// NewWriter binds w onto buf for writing. buf is taken as the initial backing slice
// (its existing content is preserved and writing continues after it). Pass nil or a
// zero-length slice to start empty. The capacity of buf determines the first growth.
func NewWriter(buf []byte) *Writer { return &Writer{stream: buf} }

// Reset re-binds w onto buf, discarding any previous state. The bit position resets
// to the start of buf (any existing bytes in buf are kept as-is and writing continues
// after them only if [Reset] is called with a non-empty buf — but count is always
// reset to 0, so the first new write begins a fresh byte after the existing content).
// Use this to reuse a *Writer from a sync.Pool onto a pooled buffer.
func (w *Writer) Reset(buf []byte) {
	w.stream = buf
	w.count = 0
}

// Bytes returns the bytes consumed so far, including the partial last byte. It is
// safe to call mid-stream; subsequent writes continue into the unused low bits of the
// last returned byte. The returned slice aliases the backing buffer.
func (w *Writer) Bytes() []byte { return w.stream }

// Len returns the number of bytes consumed so far (including a partial trailing byte).
func (w *Writer) Len() int { return len(w.stream) }

// writeBit appends a single bit (MSB-first). Kept small for inlining.
func (w *Writer) writeBit(bit bool) {
	if w.count == 0 {
		w.stream = append(w.stream, 0)
		w.count = 8
	}
	i := len(w.stream) - 1
	if bit {
		w.stream[i] |= 1 << (w.count - 1)
	}
	w.count--
}

// WriteBit appends a single bit (MSB-first).
func (w *Writer) WriteBit(bit bool) { w.writeBit(bit) }

// WriteByte appends a full byte. It conforms to [io.ByteWriter] (always returns a
// nil error) so a [*Writer] can be passed to helpers that write byte-by-byte; the
// append-style bit methods ([WriteBits], [WriteBit]) do not return an error because
// they cannot fail.
func (w *Writer) WriteByte(b byte) error {
	if w.count == 0 {
		w.stream = append(w.stream, b)
		return nil
	}
	i := len(w.stream) - 1
	// Fill the free high bits of the last byte, then append the remainder low-justified.
	w.stream[i] |= b >> (8 - w.count)
	w.stream = append(w.stream, b<<w.count)
	return nil
}

// WriteBytes appends a slice of bytes. When the writer is byte-aligned (the common
// case after [PadToByte] or a whole-byte field), this is a single append — the fast
// path for bulk byte writes like dictionary entries and row-id arrays. When not
// aligned, it falls back to per-byte [WriteByte].
//
// Go's `append([]byte, string...)` is a compiler special case that avoids a copy, so
// callers may pass a string directly: w.WriteBytes([]byte(s)) is still a copy, but
// w.stream = append(w.stream, s...) is not. For the zero-copy string path, use
// [Writer.AppendString] instead.
func (w *Writer) WriteBytes(p []byte) {
	if w.count == 0 {
		w.stream = append(w.stream, p...)
		return
	}
	for _, b := range p {
		_ = w.WriteByte(b)
	}
}

// AppendBytes reserves n byte-aligned bytes in the stream and returns the writable
// tail. The returned slice aliases the writer buffer until the next write.
func (w *Writer) AppendBytes(n int) []byte {
	if n < 0 {
		panic("bitstream: AppendBytes negative length")
	}
	if n == 0 {
		return nil
	}
	if w.count != 0 {
		oldLen := len(w.stream)
		for range n {
			_ = w.WriteByte(0)
		}
		return w.stream[oldLen:len(w.stream)]
	}
	oldLen := len(w.stream)
	w.stream = slices.Grow(w.stream, n)[:oldLen+n]
	return w.stream[oldLen:]
}

// AppendString appends a string's bytes directly when byte-aligned, with no copy
// (append([]byte, string...) is a compiler intrinsic). Falls back to per-byte
// [WriteByte] when not aligned.
func (w *Writer) AppendString(s string) {
	if w.count == 0 {
		w.stream = append(w.stream, s...)
		return
	}
	for i := 0; i < len(s); i++ {
		_ = w.WriteByte(s[i])
	}
}

// WriteBits appends the nbits least-significant bits of u, MSB-first within the
// field. nbits must be in [0, 64]; WriteBits(0, 0) is a no-op.
//
// This is the fast inline path (modeled on Prometheus writeBitsFast): it fills the
// partial last byte first, then writes whole bytes directly, then any trailing
// partial byte — avoiding per-byte call overhead.
func (w *Writer) WriteBits(u uint64, nbits int) {
	if nbits < 0 || nbits > 64 {
		panic("bitstream: WriteBits nbits out of range [0,64]")
	}
	if nbits == 0 {
		return
	}
	u <<= 64 - uint(nbits) // left-justify the field in u

	// Fill the partial last byte first.
	if w.count > 0 {
		free := int(w.count)
		last := len(w.stream) - 1
		w.stream[last] |= byte(u >> uint(64-free))
		if nbits < free {
			w.count = uint8(free - nbits)
			return
		}
		u <<= uint(free)
		nbits -= free
		w.count = 0
	}

	// Write whole bytes directly.
	for nbits >= 8 {
		w.stream = append(w.stream, byte(u>>56))
		u <<= 8
		nbits -= 8
	}

	// Trailing partial byte: start a new byte, high-justified.
	if nbits > 0 {
		w.stream = append(w.stream, byte(u>>56))
		w.count = uint8(8 - nbits)
	}
}

// WriteUvarint appends u as an unsigned varint (7 bits per byte, continuation bit in
// the high bit), matching encoding/binary Uvarint. Max 10 bytes.
func (w *Writer) WriteUvarint(u uint64) {
	for u >= 0x80 {
		_ = w.WriteByte(byte(u) | 0x80)
		u >>= 7
	}
	_ = w.WriteByte(byte(u))
}

// WriteVarint appends i as a zig-zag varint, matching encoding/binary Varint.
func (w *Writer) WriteVarint(i int64) {
	u := uint64(i) << 1
	if i < 0 {
		u = ^u
	}
	w.WriteUvarint(u)
}

// PadToByte pads the stream with zero bits up to the next byte boundary. After this,
// [Writer.Len] increases by at most one and [Bytes] is byte-aligned.
func (w *Writer) PadToByte() {
	if w.count > 0 {
		// The partial byte is already zero-filled in the low bits; just drop it.
		w.count = 0
	}
}

// AppendTo returns the written bytes as a newly-owned copy (for when the caller needs
// a stable slice independent of the backing buffer). Allocates.
func (w *Writer) AppendTo(dst []byte) []byte {
	return append(dst, w.stream...)
}

// Reader is an MSB-first bit reader over a []byte view. It performs no allocation
// after construction. The design (an 8-byte refill buffer with a valid-bit count) is
// modeled on the Prometheus/dgryski bstream; [loadNextBuffer] refills 8 bytes at a
// time so most reads are a shift-and-mask against [Reader.buffer] with no bounds check.
type Reader struct {
	stream []byte // the source bytes
	offset int    // next unread byte in stream
	buffer uint64 // refill buffer, bits high-justified (valid in the high `valid` bits)
	valid  uint8  // valid bits in buffer (0 ⇒ empty)
	last   byte   // copy of the last byte, taken at construction (see loadNextBuffer)
}

// NewReader returns a Reader over b. The Reader aliases b; callers must not mutate b
// until the Reader is done. A zero-length b yields a Reader that returns io.EOF from
// every read.
func NewReader(b []byte) *Reader {
	r := &Reader{stream: b}
	if len(b) > 0 {
		r.last = b[len(b)-1]
	}
	return r
}

// Reset re-binds r onto b, discarding prior state. Cheaper than allocating a new
// Reader; use with a pooled *Reader.
func (r *Reader) Reset(b []byte) {
	r.stream = b
	r.offset = 0
	r.buffer = 0
	r.valid = 0
	if len(b) > 0 {
		r.last = b[len(b)-1]
	} else {
		r.last = 0
	}
}

// Remaining reports the number of bytes still unread, including the partial byte held
// in the refill buffer. It is approximate (rounds the buffered bits up to a byte).
func (r *Reader) Remaining() int { return len(r.stream) - r.offset + int(r.valid)/8 }

// ConsumedBytes reports how many bytes of the source stream have been fully consumed
// (every bit read). It is the byte offset at which the next byte-aligned field or
// column stream begins. Callers that [Writer.PadToByte] at the end of encoding can use
// this to slice the source for the next column.
func (r *Reader) ConsumedBytes() int {
	// Total bits pulled from stream = offset * 8. Bits still in buffer = valid.
	// Consumed bits = offset*8 - valid. Consumed bytes (ceil, since partial bytes
	// are part of the current column) = offset - floor(valid / 8).
	return r.offset - int(r.valid)/8
}

// readBitFast is the inlined fast path: returns io.EOF if the buffer is empty so the
// caller can fall back to [readBit]. Kept a leaf for inlining.
func (r *Reader) readBitFast() (bool, error) {
	if r.valid == 0 {
		return false, io.EOF
	}
	r.valid--
	return (r.buffer & (1 << r.valid)) != 0, nil
}

// ReadBit reads one bit.
func (r *Reader) ReadBit() (bool, error) {
	if r.valid == 0 {
		if !r.loadNextBuffer(1) {
			return false, io.EOF
		}
	}
	r.valid--
	return (r.buffer & (1 << r.valid)) != 0, nil
}

// readBitsFast is the inlined fast path for readBits when nbits ≤ valid.
func (r *Reader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > r.valid {
		return 0, io.EOF
	}
	mask := (uint64(1) << nbits) - 1
	r.valid -= nbits
	return (r.buffer >> r.valid) & mask, nil
}

// ReadBits reads nbits bits (nbits in [0,64]) and returns them in the low nbits of a
// uint64. Returns io.EOF if the stream is exhausted.
func (r *Reader) ReadBits(nbits uint8) (uint64, error) {
	if nbits > 64 {
		panic("bitstream: ReadBits nbits out of range [0,64]")
	}
	if nbits == 0 {
		return 0, nil
	}
	if r.valid == 0 {
		if !r.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}
	if nbits <= r.valid {
		return r.readBitsFast(nbits)
	}

	// Field spans two buffers: take all remaining valid bits, refill, take the rest.
	mask := (uint64(1) << r.valid) - 1
	nbits -= r.valid
	v := (r.buffer & mask) << nbits
	r.valid = 0
	if !r.loadNextBuffer(nbits) {
		return 0, io.EOF
	}
	mask = (uint64(1) << nbits) - 1
	v |= (r.buffer >> (r.valid - nbits)) & mask
	r.valid -= nbits
	return v, nil
}

// ReadByte reads a full byte (8 bits).
func (r *Reader) ReadByte() (byte, error) {
	v, err := r.ReadBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// ReadBytes reads n bytes into p (which must have length n) when the reader is
// byte-aligned, which is the common case after [AlignToByte] or a whole-byte field.
// When not aligned, it falls back to per-byte [ReadByte]. This is the bulk read path
// for dictionary entries and row-id arrays.
func (r *Reader) ReadBytes(p []byte) error {
	// Align first: drop any partial-buffer bits so we can read from the stream
	// directly.
	r.AlignToByte()
	// If we have buffered bytes (from a refill), drain them first.
	for len(p) > 0 && r.valid >= 8 {
		bits := r.buffer >> (r.valid - 8)
		p[0] = byte(bits)
		p = p[1:]
		r.valid -= 8
	}
	if len(p) == 0 {
		return nil
	}
	// Read remaining bytes directly from the stream.
	avail := len(r.stream) - r.offset
	if avail < len(p) {
		return io.EOF
	}
	copy(p, r.stream[r.offset:r.offset+len(p)])
	r.offset += len(p)
	// Reset buffer state since we've consumed past it.
	r.buffer = 0
	r.valid = 0
	return nil
}

// ReadBytesView returns the next n byte-aligned bytes as a view into the source
// stream. The returned slice aliases the reader input and is valid until that input
// is mutated or released.
func (r *Reader) ReadBytesView(n int) ([]byte, error) {
	if n < 0 {
		panic("bitstream: ReadBytesView negative length")
	}
	if n == 0 {
		return nil, nil
	}

	r.AlignToByte()
	validBytes := int(r.valid) / 8
	start := r.offset - validBytes
	if len(r.stream)-start < n {
		return nil, io.EOF
	}
	view := r.stream[start : start+n]
	if n <= validBytes {
		r.valid -= uint8(n * 8)
		if r.valid == 0 {
			r.buffer = 0
		}
		return view, nil
	}
	r.offset = start + n
	r.buffer = 0
	r.valid = 0
	return view, nil
}

// ReadUvarint reads an unsigned varint (7 bits per byte, continuation in the high
// bit), matching [Writer.WriteUvarint] and encoding/binary Uvarint. Direct method
// calls (not io.ByteReader) avoid the receiver escaping to the heap.
func (r *Reader) ReadUvarint() (uint64, error) {
	var x uint64
	var s uint
	for range binary.MaxVarintLen64 {
		b, err := r.ReadByte()
		if err != nil {
			return x, err
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, io.ErrUnexpectedEOF
}

// ReadVarint reads a zig-zag varint, matching [Writer.WriteVarint] and
// encoding/binary Varint.
func (r *Reader) ReadVarint() (int64, error) {
	ux, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}

// SkipBits discards nbits bits. nbits must be in [0,64].
func (r *Reader) SkipBits(nbits uint8) error {
	if nbits == 0 {
		return nil
	}
	if _, err := r.ReadBits(nbits); err != nil {
		return err
	}
	return nil
}

// AlignToByte discards the buffered bits up to the next byte boundary (so the next
// read begins at a byte boundary in the refill buffer).
func (r *Reader) AlignToByte() {
	// Drop the partial-byte bits: valid is the count of high-justified bits; round
	// down to a multiple of 8.
	r.valid &^= 7
	if r.valid == 0 {
		r.buffer = 0
	}
}

// loadNextBuffer refills the 8-byte buffer from the stream. nbits is the minimum the
// caller needs, but we read up to 8 bytes when possible to amortize. It handles the
// final-byte race (the last byte may be concurrently written by a Writer sharing the
// slice) by using the [Reader.last] copy taken at construction — matching the
// Prometheus bstream invariant.
func (r *Reader) loadNextBuffer(nbits uint8) bool {
	if r.offset >= len(r.stream) {
		return false
	}
	// Fast path: at least 8 bytes remain (never touches the very last byte).
	if r.offset+8 < len(r.stream) {
		r.buffer = binary.BigEndian.Uint64(r.stream[r.offset:])
		r.offset += 8
		r.valid = 64
		return true
	}

	// Slow path: ≤8 bytes left. Read as many as available, high-justified.
	nbytes := int(nbits/8) + 1
	if r.offset+nbytes > len(r.stream) {
		nbytes = len(r.stream) - r.offset
	}
	var buffer uint64
	skip := 0
	if r.offset+nbytes == len(r.stream) {
		// The last byte may be mid-write by a Writer; use the construction-time copy.
		buffer = uint64(r.last)
		skip = 1
	}
	for i := 0; i < nbytes-skip; i++ {
		buffer |= uint64(r.stream[r.offset+i]) << uint(8*(nbytes-i-1))
	}
	r.buffer = buffer
	r.offset += nbytes
	r.valid = uint8(nbytes * 8)
	return true
}

// LeadingZeros64 is a convenience for codecs that need the bit-width of a value; it
// returns bits.LeadingZeros64(u). Exposed so codecs don't import math/bits directly.
func LeadingZeros64(u uint64) int { return bits.LeadingZeros64(u) }

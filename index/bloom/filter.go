// Package bloom is a token bloom filter for full-text pruning: a compact, approximate set of the
// terms present in a column block (e.g. every token across a log part's bodies). A query token
// that tests **absent** is definitely not in the block, so the reader skips the whole block; a
// token that tests present may be a false positive, so the engine re-checks the exact predicate
// per row. The filter never reports a false negative — a token that was [Filter.Add]ed always
// [Filter.Test]s present — which is what makes block-skipping safe.
package bloom

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"
	"github.com/zeebo/xxh3"
)

// Filter is a bit-array bloom filter with k hash probes derived from one 128-bit hash by
// double-hashing (Kirsch–Mitzenmacher). The zero value is not usable; build one with [New].
type Filter struct {
	bits []uint64
	m    uint64 // number of bits (= len(bits)*64), always a positive multiple of 64
	k    int    // number of probes per item
}

// encodeVersion is the on-disk format tag of an encoded filter.
const encodeVersion byte = 1

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// New returns a filter sized for n expected items at false-positive rate p (0 < p < 1), using the
// standard m = -n·ln p / (ln2)² and k = (m/n)·ln2, with m rounded up to a multiple of 64 and both
// m and k clamped to at least their minimums.
func New(n int, p float64) *Filter {
	if n < 1 {
		n = 1
	}

	if p <= 0 || p >= 1 {
		p = 0.01
	}

	m := max(uint64(math.Ceil(-float64(n)*math.Log(p)/(math.Ln2*math.Ln2))), 64)
	m = (m + 63) &^ 63 // round up to a whole 64-bit word

	k := max(int(math.Round(float64(m)/float64(n)*math.Ln2)), 1)

	return &Filter{bits: make([]uint64, m/64), m: m, k: k}
}

// Add records item in the filter.
func (f *Filter) Add(item []byte) {
	h1, h2 := hashes(item)
	for i := range f.k {
		pos := (h1 + uint64(i)*h2) % f.m
		f.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Test reports whether item may be present: true if every probe bit is set (possibly a false
// positive), false if any is clear (definitely absent — no false negatives).
func (f *Filter) Test(item []byte) bool {
	h1, h2 := hashes(item)
	for i := range f.k {
		pos := (h1 + uint64(i)*h2) % f.m
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}

	return true
}

func hashes(item []byte) (uint64, uint64) {
	h := xxh3.Hash128(item)

	return h.Hi, h.Lo
}

// Encode appends the filter's self-describing wire form to dst:
// [version][uvarint k][uvarint m][bits little-endian]…[u32 CRC32C of the preceding bytes].
func (f *Filter) Encode(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, encodeVersion)
	dst = binary.AppendUvarint(dst, uint64(f.k))
	dst = binary.AppendUvarint(dst, f.m)

	for _, w := range f.bits {
		dst = binary.LittleEndian.AppendUint64(dst, w)
	}

	crc := crc32.Checksum(dst[start:], castagnoli)

	return binary.LittleEndian.AppendUint32(dst, crc)
}

// Decode parses a filter encoded by [Filter.Encode], returning it and the number of bytes
// consumed. It is fully bounds-checked and verifies the trailing CRC.
func Decode(src []byte) (*Filter, int, error) {
	if len(src) < 1 || src[0] != encodeVersion {
		return nil, 0, errors.New("bloom: bad version")
	}

	off := 1

	k64, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return nil, 0, errors.New("bloom: bad k")
	}

	off += n

	m, n := binary.Uvarint(src[off:])
	if n <= 0 || m == 0 || m%64 != 0 {
		return nil, 0, errors.New("bloom: bad m")
	}

	off += n

	words := int(m / 64)
	if len(src)-off < words*8+4 {
		return nil, 0, errors.New("bloom: truncated")
	}

	bits := make([]uint64, words)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(src[off+i*8:])
	}

	end := off + words*8
	if crc32.Checksum(src[:end], castagnoli) != binary.LittleEndian.Uint32(src[end:end+4]) {
		return nil, 0, errors.New("bloom: CRC mismatch")
	}

	return &Filter{bits: bits, m: m, k: int(k64)}, end + 4, nil
}

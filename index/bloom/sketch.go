package bloom

import (
	"math"
	"math/bits"

	"github.com/zeebo/xxh3"
)

// sketchPrecision is the number of hash bits used as a register index: 2^12 = 4096 one-byte
// registers (4 KiB, reused per column build) for a ~1.6% standard error — far tighter than the
// accuracy a filter's size needs, and constant regardless of how many tokens are counted.
const sketchPrecision = 12

const sketchRegisters = 1 << sketchPrecision

// Sketch estimates how many *distinct* items a stream contains, in constant space
// (HyperLogLog). A bloom filter must be sized by its distinct item count: sizing by the number of
// occurrences over-allocates by the average repetition factor, which for tokenized log text is
// one to two orders of magnitude (the same words recur in every row). The zero value is ready to
// use; [Sketch.Reset] re-arms it.
type Sketch struct {
	reg [sketchRegisters]uint8
}

// Reset clears the sketch for a fresh count.
func (s *Sketch) Reset() { s.reg = [sketchRegisters]uint8{} }

// Add records item.
func (s *Sketch) Add(item []byte) { s.AddHash(xxh3.Hash(item)) }

// AddHash records an item by its hash, for a caller that already hashed it (e.g. through
// [Hashes]). Any well-distributed 64-bit hash works, as long as one item always yields one hash.
func (s *Sketch) AddHash(h uint64) {
	idx := h >> (64 - sketchPrecision)
	// Rank of the first set bit among the remaining bits; the trailing sentinel bit bounds the
	// rank when the remainder is all zeros.
	rank := uint8(bits.LeadingZeros64(h<<sketchPrecision|1<<(sketchPrecision-1))) + 1

	if rank > s.reg[idx] {
		s.reg[idx] = rank
	}
}

// Estimate returns the approximate number of distinct items added, using the standard
// HyperLogLog estimator with linear counting on the sparse (small-cardinality) range.
func (s *Sketch) Estimate() int {
	const m = float64(sketchRegisters)

	alpha := 0.7213 / (1 + 1.079/m)

	var (
		sum   float64
		zeros int
	)

	for _, r := range &s.reg {
		sum += math.Ldexp(1, -int(r))
		if r == 0 {
			zeros++
		}
	}

	est := alpha * m * m / sum

	// Small-range correction: with empty registers left, linear counting is the better estimator.
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}

	return int(est)
}

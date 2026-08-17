//go:build !race

// The split-form encode's allocation guard, measured only in an uninstrumented build: the race
// detector adds shadow allocations of its own (a steady 3 per encode), which say nothing about the
// encoder.

package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEncodeBytesDictNoAlloc pins the steady state: with a caller-owned buffer, encoding a granule
// from the split form allocates nothing.
//
// Measured over a benchmark rather than with [testing.AllocsPerRun], because the scratch comes from
// a [sync.Pool]: a GC empties the pool, so a collection landing between two of a handful of
// iterations turns the next Get into an allocation and the assertion into a measure of GC timing.
// Over a benchmark's iteration count those refills divide away, and what is left is the per-encode
// allocation the test is about. It stays serial for the same reason — the package's parallel tests
// allocate enough to drain the pool underneath it.
//
//nolint:paralleltest // an allocation measurement must not share the process with parallel tests.
func TestEncodeBytesDictNoAlloc(t *testing.T) {
	entries := distinctEntries(300)
	ids := cyclicIDs(8192, 300)

	r := testing.Benchmark(func(b *testing.B) {
		b.Helper()

		buf := make([]byte, 0, len(EncodeBytesDict(nil, entries, ids)))

		for b.Loop() {
			buf = EncodeBytesDict(buf[:0], entries, ids)
		}
	})

	assert.Zero(t, r.AllocsPerOp())
}

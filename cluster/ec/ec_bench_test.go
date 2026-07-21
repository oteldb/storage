package ec_test

import (
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/cluster/ec"
)

// objectSize is the benchmark payload: a typical flushed column object.
const objectSize = 1 << 20

func benchData(b *testing.B) []byte {
	b.Helper()

	rng := rand.New(rand.NewPCG(1, 2))
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(rng.Uint32())
	}

	return data
}

func BenchmarkEncode(b *testing.B) {
	data := benchData(b)
	s := ec.Scheme{Data: 4, Parity: 2}

	b.ReportAllocs()
	b.SetBytes(objectSize) // logical, uncompressed bytes on both sides

	for b.Loop() {
		if _, err := ec.Encode(s, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReconstructJoin(b *testing.B) {
	data := benchData(b)
	s := ec.Scheme{Data: 4, Parity: 2}

	shards, err := ec.Encode(s, data)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(objectSize) // logical bytes reconstructed per op

	for b.Loop() {
		// Worst case: Parity data shards lost (parity shards must be folded back in).
		work := make([][]byte, len(shards))
		copy(work, shards)
		work[0], work[1] = nil, nil

		if err := ec.Reconstruct(s, work); err != nil {
			b.Fatal(err)
		}

		if _, err := ec.Join(s, work, objectSize); err != nil {
			b.Fatal(err)
		}
	}
}

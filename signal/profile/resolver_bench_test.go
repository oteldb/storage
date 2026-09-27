package profile

import (
	"testing"
)

// BenchmarkResolverBuild is the per-query resolver path on a cache miss: snapshot the head, decode a
// flushed part's sidecars as the backend returns them, and layer the two into a resolver.
func BenchmarkResolverBuild(b *testing.B) {
	corpus := pprofCorpus(b)

	s := NewSymbolStore()
	if err := s.Absorb(encodeDelta(corpus)); err != nil {
		b.Fatal(err)
	}

	var logical int64
	for _, t := range corpus.t {
		for _, entry := range t {
			logical += 16 + int64(len(entry))
		}
	}

	part := storedSidecars(b, s)

	for _, bc := range []struct {
		name string
		part bool
	}{
		{"Head", false},
		{"HeadAndPart", true},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				layers := []*Tables{s.Tables()}

				if bc.part {
					t, err := DecodeTables(part)
					if err != nil {
						b.Fatal(err)
					}

					layers = append(layers, t)
				}

				NewResolverFrom(layers...)
			}
		})
	}
}

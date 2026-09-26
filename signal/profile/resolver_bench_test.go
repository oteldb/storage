package profile

import (
	"testing"
)

// BenchmarkResolverBuild is the per-query resolver path: union the head with a flushed part's
// sidecars as the backend returns them, then decode the union into a resolver.
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
		name  string
		parts func() []map[string][]byte
	}{
		{"Head", func() []map[string][]byte { return []map[string][]byte{s.Encode()} }},
		{"HeadAndPart", func() []map[string][]byte { return []map[string][]byte{s.Encode(), part} }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(logical)
			b.ReportAllocs()

			for b.Loop() {
				union, err := NewSymbolStore().Union(bc.parts())
				if err != nil {
					b.Fatal(err)
				}

				if _, err := NewResolver(union); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

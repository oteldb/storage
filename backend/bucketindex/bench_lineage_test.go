package bucketindex_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend/bucketindex"
)

const (
	benchChains = 500
	benchDepth  = 8
)

// nestedLineage is a catalog near [bucketindex.MaxLineage] of deeply nested groups: benchChains
// ancestors, each split benchDepth times, every split taking the previous split's second member. It
// returns the catalog, the innermost member of each chain, and each chain's ancestor block.
func nestedLineage() (catalog []bucketindex.Claim, inner []bucketindex.Want, ancestors []uint64) {
	next := uint64(1)
	take := func() uint64 { next++; return next - 1 }

	for c := range benchChains {
		anc := take()
		ancestors = append(ancestors, anc)
		consumed := bucketindex.Blocks(anc)

		var member uint64

		for d := range benchDepth {
			a, b := take(), take()
			claim := bucketindex.Claim{Blocks: consumed, Group: bucketindex.Blocks(a, b)}
			catalog = append(catalog, claim)
			consumed, member = bucketindex.Blocks(b), b

			if d == benchDepth-1 {
				inner = append(inner, bucketindex.Want{
					Prefix: fmt.Sprintf("inner-%04d", c), Blocks: bucketindex.Blocks(member),
					Claim: claim, Level: uint32(d + 1),
				})
			}
		}
	}

	return bucketindex.MergeLineage(catalog), inner, ancestors
}

// nestedLive is a shard's live set over the nested lineage: 100 parts, each a high-level successor
// of five chains' ancestors — so it answers those chains' inner wants only through the whole chain.
func nestedLive(ancestors []uint64) []bucketindex.Entry {
	live := make([]bucketindex.Entry, 0, 100)

	for i := range 100 {
		live = append(live, bucketindex.Entry{
			Prefix: fmt.Sprintf("succ-%03d", i),
			Blocks: bucketindex.Blocks(ancestors[5*i : 5*i+5]...),
			Level:  benchDepth + 5,
		})
	}

	return live
}

func BenchmarkTrimWantsNestedLineage(b *testing.B) {
	catalog, inner, ancestors := nestedLineage()
	live := nestedLive(ancestors)

	for _, n := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("wants=%d", n), func(b *testing.B) {
			wants := inner[:n]
			b.ReportAllocs()

			for b.Loop() {
				bucketindex.TrimWants(append([]bucketindex.Want(nil), wants...), live, catalog...)
			}
		})
	}
}

// BenchmarkSatisfyingWithNestedLineage is a peer answering a batch of repair wants with the wanting
// node's lineage, as partsync does: the relations built once per peer, then asked per want.
func BenchmarkSatisfyingWithNestedLineage(b *testing.B) {
	catalog, inner, ancestors := nestedLineage()
	peer := &bucketindex.Index{Entries: nestedLive(ancestors)}

	for _, n := range []int{1, 16} {
		b.Run(fmt.Sprintf("wants=%d", n), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				rel := peer.Relations(catalog...)
				for i := range n {
					rel.Satisfying(inner[i])
				}
			}
		})
	}
}

// nestedCompletion is, for the first n chains of [nestedLineage], the parts completing each chain's
// outermost group: the first member of every split and both innermost members, so every group of
// those chains is complete, the outer ones only through the ones nested in them.
func nestedCompletion(n int) []bucketindex.Entry {
	var out []bucketindex.Entry

	for c := range n {
		anc := 1 + uint64(c)*(1+2*benchDepth)
		consumed := bucketindex.Blocks(anc)

		for d := range benchDepth {
			a := anc + 1 + 2*uint64(d)
			claim := bucketindex.Claim{Blocks: consumed, Group: bucketindex.Blocks(a, a+1)}
			consumed = bucketindex.Blocks(a + 1)

			members := []uint64{a}
			if d == benchDepth-1 {
				members = append(members, a+1)
			}

			for _, m := range members {
				out = append(out, bucketindex.Entry{
					Prefix: fmt.Sprintf("c%04d-%d", c, m), Blocks: bucketindex.Blocks(m), Claim: claim, Level: uint32(d + 1),
				})
			}
		}
	}

	return out
}

// BenchmarkSubsumedNestedLineage is a repair commit's pruning: the parts completing n nested chains
// judged against the live set over the whole catalog.
func BenchmarkSubsumedNestedLineage(b *testing.B) {
	catalog, _, ancestors := nestedLineage()
	live := nestedLive(ancestors)

	for _, n := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("chains=%d", n), func(b *testing.B) {
			added := nestedCompletion(n)
			all := append(append([]bucketindex.Entry(nil), live...), added...)
			b.ReportAllocs()

			for b.Loop() {
				bucketindex.LineageOf(all).With(catalog...).Relater().Subsumed(live, added)
			}
		})
	}
}

// BenchmarkRecordLineageSaturated is a commit's catalog upkeep past the target: the nested catalog
// with 200 newer groups nothing reaches, and a live set of every chain's innermost member beside the
// successors, so trimming walks each live part's whole ancestry.
func BenchmarkRecordLineageSaturated(b *testing.B) {
	catalog, inner, ancestors := nestedLineage()

	for i := range uint64(200) {
		base := uint64(1 << 20)
		catalog = append(catalog, bucketindex.Claim{
			Blocks: bucketindex.Blocks(base + 2*i), Group: bucketindex.Blocks(base + 2*i + 1),
		})
	}

	catalog = bucketindex.MergeLineage(catalog)
	live := nestedLive(ancestors)

	for i := range inner {
		live = append(live, inner[i].Entry())
	}

	b.ReportAllocs()

	for b.Loop() {
		ix := &bucketindex.Index{Entries: live, Catalog: slices.Clone(catalog)}
		ix.RecordLineage()
	}
}

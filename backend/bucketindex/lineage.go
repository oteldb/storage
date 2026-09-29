package bucketindex

import "slices"

// Lineage is the split groups a set of entries records. It relates identities their own blocks do
// not: a member's blocks are fresh, so only its group's [Claim] says which ancestor rows it holds.
type Lineage []Claim

// LineageOf collects the distinct claims the data-bearing entries carry.
func LineageOf(entries []Entry) Lineage {
	var out Lineage

	for i := range entries {
		c := entries[i].Claim
		if entries[i].Data() && c.Valid() && !slices.ContainsFunc(out, c.Equal) {
			out = append(out, c)
		}
	}

	return out
}

// Subsumes reports whether e holds every row o holds, at a higher level: e covers o's blocks, or
// the whole ancestry of the groups o's blocks belong to.
//
// Two parts with one identity written by different tenures — two owners that merged the same inputs
// either side of a handoff — hold the same rows, and the later tenure's copy subsumes the earlier.
// A caller relating many pairs over one lineage uses a [Relater] instead.
func (l Lineage) Subsumes(e, o Entry) bool { return l.Relater().Subsumes(e, o) }

func (e Entry) sameIdentity(o Entry) bool {
	return e.Level == o.Level && e.Blocks.Valid() && e.Blocks.Equal(o.Blocks) && e.Claim.Equal(o.Claim)
}

// Overlaps reports whether a and b may hold a row in common.
//
// Rows are shared only along descent: a block and a block derived from it overlap, as does a block
// with itself. A split partitions the rows it consumed, so two parts descending from one ancestor
// through different members of the same group are disjoint; through different groups — two lineages
// that split the same ancestor apart differently — nothing says how the rows fell, and they may
// overlap. A caller relating many pairs over one lineage uses a [Relater] instead.
func (l Lineage) Overlaps(a, b Entry) bool { return l.Relater().Overlaps(a, b) }

func meets(a, b Interval) bool { return intersect(a, b).Valid() }

// intersect is the set of blocks both cover, unset when they share none.
func intersect(a, b Interval) Interval {
	if !a.Valid() || !b.Valid() {
		return Interval{}
	}

	var out []Gap

	for i, j := 0, 0; i < a.nruns() && j < b.nruns(); {
		ra, rb := a.run(i), b.run(j)
		if lo, hi := MaxBlock(ra.Min, rb.Min), minBlock(ra.Max, rb.Max); !hi.less(lo) {
			out = append(out, Gap{Min: lo, Max: hi})
		}

		if ra.Max.less(rb.Max) {
			i++
		} else {
			j++
		}
	}

	return fromRuns(out)
}

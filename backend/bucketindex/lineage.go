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
func (l Lineage) Subsumes(e, o Entry) bool {
	return e.Level > o.Level && l.holds(e.Blocks).Contains(o.Blocks)
}

// Overlaps reports whether a and b may hold a row in common.
//
// Rows are shared only along descent: a block and a block derived from it overlap, as does a block
// with itself. A split partitions the rows it consumed, so two parts descending from one ancestor
// through different members of the same group are disjoint; through different groups — two lineages
// that split the same ancestor apart differently — nothing says how the rows fell, and they may
// overlap.
func (l Lineage) Overlaps(a, b Entry) bool {
	upA, viaA := l.ancestry(a.Blocks)
	upB, viaB := l.ancestry(b.Blocks)

	if meets(a.Blocks, upB) || meets(b.Blocks, upA) {
		return true
	}

	common := intersect(upA, upB)
	if !common.Valid() {
		return false
	}

	for i, c := range l {
		if viaA[i] != viaB[i] && meets(c.Blocks, common) {
			return true
		}
	}

	return false
}

// holds is every block whose rows iv includes: iv, and the members of each group whose claimed
// ancestry it covers, repeated so a group split again resolves through the outer one.
func (l Lineage) holds(iv Interval) Interval {
	used := make([]bool, len(l))

	for changed := true; changed; {
		changed = false

		for i, c := range l {
			if !used[i] && iv.Contains(c.Blocks) {
				used[i], changed = true, true
				iv = iv.Union(c.Group)
			}
		}
	}

	return iv
}

// ancestry is iv with every block its rows derive from, and which groups that descent passes
// through.
func (l Lineage) ancestry(iv Interval) (Interval, []bool) {
	via := make([]bool, len(l))

	for changed := true; changed; {
		changed = false

		for i, c := range l {
			if !via[i] && meets(iv, c.Group) {
				via[i], changed = true, true
				iv = iv.Union(c.Blocks)
			}
		}
	}

	return iv, via
}

func meets(a, b Interval) bool { return intersect(a, b).Valid() }

// intersect is the set of blocks both cover, unset when they share none.
func intersect(a, b Interval) Interval {
	if !a.Valid() || !b.Valid() {
		return Interval{}
	}

	ra, rb := a.runs(), b.runs()

	var out []Gap

	for i, j := 0, 0; i < len(ra) && j < len(rb); {
		if lo, hi := max(ra[i].Min, rb[j].Min), min(ra[i].Max, rb[j].Max); lo <= hi {
			out = append(out, Gap{Min: lo, Max: hi})
		}

		if ra[i].Max < rb[j].Max {
			i++
		} else {
			j++
		}
	}

	return fromRuns(out)
}

package repair

import (
	"cmp"
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// pruned is what a commit of the units taken does to the part set.
type pruned struct {
	// kept are the prefixes of the parts it publishes; retired, those of the live parts it retires.
	kept, retired map[string]struct{}
	// drop are the units it cannot take after all, for reason.
	drop   []int
	reason string
}

// prune decides which parts of the units in the commit publishes, which live parts it retires, and
// which units it must drop first: one no longer answered once subsumed parts are left out, or one
// whose published part overlaps another part left live (engine/ARCH.md, "Repair"). Subsumption is
// judged over the lineage of everything the commit saw: a member left out may be the only record
// relating what remains.
func prune(
	live []bucketindex.Entry, units []Unit, added [][]bucketindex.Entry, openErr map[string]error, in []bool,
) pruned {
	var admitted []bucketindex.Entry

	for k := range units {
		if !in[k] {
			continue
		}

		for i := range added[k] {
			p := added[k][i].Prefix
			if !slices.ContainsFunc(admitted, func(a bucketindex.Entry) bool { return a.Prefix == p }) {
				admitted = append(admitted, added[k][i])
			}
		}
	}

	all := slices.Concat(live, admitted)
	lineage := bucketindex.LineageOf(all)
	subsumed := lineage.Subsumed(admitted, all)
	published := slices.DeleteFunc(admitted, func(e bucketindex.Entry) bool {
		_, ok := subsumed[e.Prefix]

		return ok
	})

	out := pruned{
		kept:    make(map[string]struct{}, len(published)),
		retired: lineage.Subsumed(live, published),
	}

	for i := range published {
		out.kept[published[i].Prefix] = struct{}{}
	}

	final := slices.DeleteFunc(slices.Clone(live), func(e bucketindex.Entry) bool {
		_, ok := out.retired[e.Prefix]

		return ok
	})
	final = append(final, published...)

	own := make([][]bucketindex.Entry, len(units))

	for k := range units {
		if !in[k] {
			continue
		}

		own[k] = slices.DeleteFunc(slices.Clone(added[k]), func(e bucketindex.Entry) bool {
			_, ok := out.kept[e.Prefix]

			return !ok
		})

		base := slices.DeleteFunc(slices.Clone(final), func(e bucketindex.Entry) bool {
			return slices.ContainsFunc(own[k], func(o bucketindex.Entry) bool { return o.Prefix == e.Prefix })
		})

		if !admissible(base, units[k], own[k], openErr) {
			out.drop = append(out.drop, k)
		}
	}

	if len(out.drop) > 0 {
		out.reason = "repaired unit is not answered once parts it subsumes are left out"

		return out
	}

	out.drop = overlapping(lineage, live, units, own, final, in)
	out.reason = "repaired part overlaps rows another part holds"

	return out
}

// unitOverlaps records, per unit taken, what its published parts overlap among the parts a commit
// leaves live: in live when that is a live part or another of its own, in units when another unit
// publishes it.
type unitOverlaps struct {
	live  []bool
	units [][]bool
}

// overlapping returns the units to drop for overlap: every one overlapping a live part, since that
// representation is already in place; failing that, every one overlapping the first overlapping
// unit by target.
func overlapping(
	lineage bucketindex.Lineage, live []bucketindex.Entry, units []Unit, own [][]bucketindex.Entry,
	final []bucketindex.Entry, in []bool,
) []int {
	o := overlapsOf(lineage, live, own, final, in)

	var drop []int

	for k := range units {
		if o.live[k] {
			drop = append(drop, k)
		}
	}

	if len(drop) > 0 {
		return drop
	}

	first := -1

	for k := range units {
		if slices.Contains(o.units[k], true) && (first < 0 || compareUnits(units[k], units[first]) < 0) {
			first = k
		}
	}

	if first < 0 {
		return nil
	}

	for k, overlap := range o.units[first] {
		if overlap {
			drop = append(drop, k)
		}
	}

	return drop
}

func overlapsOf(
	lineage bucketindex.Lineage, live []bucketindex.Entry, own [][]bucketindex.Entry, final []bucketindex.Entry, in []bool,
) unitOverlaps {
	owners := make(map[string][]int)

	for i := range live {
		owners[live[i].Prefix] = []int{-1}
	}

	for k := range own {
		for i := range own[k] {
			owners[own[k][i].Prefix] = append(owners[own[k][i].Prefix], k)
		}
	}

	o := unitOverlaps{live: make([]bool, len(own)), units: make([][]bool, len(own))}

	for k := range own {
		if !in[k] {
			continue
		}

		o.units[k] = make([]bool, len(own))

		for i := range own[k] {
			a := &own[k][i]

			for j := range final {
				x := &final[j]
				if x.Prefix == a.Prefix || !x.Data() || !a.Data() || !lineage.Overlaps(*a, *x) {
					continue
				}

				for _, owner := range owners[x.Prefix] {
					if owner < 0 || owner == k {
						o.live[k] = true
					} else {
						o.units[k][owner] = true
					}
				}
			}
		}
	}

	return o
}

func compareUnits(a, b Unit) int {
	return cmp.Or(
		cmp.Compare(a[0].Want.Prefix, b[0].Want.Prefix),
		cmp.Compare(a[0].Entry.Prefix, b[0].Entry.Prefix),
	)
}

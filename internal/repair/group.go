package repair

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// committable reports whether added may be committed over base on behalf of want: together they
// answer it, and every group [groupsNeeded] names is complete.
func committable(
	base []bucketindex.Entry, want bucketindex.Want, added []bucketindex.Entry, catalog []bucketindex.Claim,
) bool {
	all := &bucketindex.Index{Entries: slices.Concat(base, added), Catalog: catalog}
	if _, ok := all.Satisfying(want); !ok {
		return false
	}

	held := all.Covered()
	for _, c := range groupsNeeded(base, want, added, catalog) {
		if !held.Contains(c.Group) {
			return false
		}
	}

	return true
}

// groupsNeeded is the split groups whose members must all be present before added may be committed
// over base. While base and added do not yet answer want, that is every group added carries, since
// one of them is what answers it. Once they do, it is the groups added's rows derive through whose
// claimed ancestry overlaps what base holds: a lone member of one duplicates rows base already has,
// and only the complete group retires them. Such a group's members split again are needed whole as
// well, since the group is complete only once they are. A member of any other group holds rows base
// lacks, and commits alone.
func groupsNeeded(
	base []bucketindex.Entry, want bucketindex.Want, added []bucketindex.Entry, catalog []bucketindex.Claim,
) []bucketindex.Claim {
	all := slices.Concat(base, added)

	_, answered := (&bucketindex.Index{Entries: all, Catalog: catalog}).Satisfying(want)
	if !answered {
		var out []bucketindex.Claim

		for i := range added {
			if c := added[i].Claim; c.Valid() && !slices.ContainsFunc(out, c.Equal) {
				out = append(out, c)
			}
		}

		return out
	}

	held := (&bucketindex.Index{Entries: base, Catalog: catalog}).Covered()
	r := bucketindex.LineageOf(all).With(catalog...).Relater()

	var derived []bucketindex.Claim

	for i := range added {
		for _, c := range r.Ancestors(added[i].Blocks) {
			if !slices.ContainsFunc(derived, c.Equal) {
				derived = append(derived, c)
			}
		}
	}

	var out []bucketindex.Claim

	for grew := true; grew; {
		grew = false

		for _, c := range derived {
			if slices.ContainsFunc(out, c.Equal) {
				continue
			}

			if overlaps(held, c.Blocks) || slices.ContainsFunc(out, func(n bucketindex.Claim) bool {
				return overlaps(n.Group, c.Blocks)
			}) {
				out, grew = append(out, c), true
			}
		}
	}

	return out
}

func overlaps(a, b bucketindex.Interval) bool {
	return a.Valid() && b.Valid() && a.Union(b).Len() < a.Len()+b.Len()
}

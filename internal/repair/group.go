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

// groupsNeeded is the split groups among added whose members must all be present before added may
// be committed over base. While base and added do not yet answer want, that is every group added
// carries, since one of them is what answers it. Once they do, it is only the groups whose claimed
// ancestry overlaps what base holds: a lone member of one duplicates rows base already has, and only
// the complete group retires them. A member of any other group holds rows base lacks, and commits
// alone.
func groupsNeeded(
	base []bucketindex.Entry, want bucketindex.Want, added []bucketindex.Entry, catalog []bucketindex.Claim,
) []bucketindex.Claim {
	_, answered := (&bucketindex.Index{Entries: slices.Concat(base, added), Catalog: catalog}).Satisfying(want)

	var held bucketindex.Interval
	if answered {
		held = (&bucketindex.Index{Entries: base, Catalog: catalog}).Covered()
	}

	var out []bucketindex.Claim

	for i := range added {
		c := added[i].Claim
		if !c.Valid() || answered && !overlaps(held, c.Blocks) || slices.ContainsFunc(out, c.Equal) {
			continue
		}

		out = append(out, c)
	}

	return out
}

func overlaps(a, b bucketindex.Interval) bool {
	return a.Valid() && b.Valid() && a.Union(b).Len() < a.Len()+b.Len()
}

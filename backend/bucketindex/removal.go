package bucketindex

import "slices"

// MaxRemovals bounds how many tombstones an index carries. A removal has to be remembered until
// every replica has seen it, and no writer can know when that is, so it is remembered for a
// bounded number of removals instead.
//
// A replica further behind than this keeps the objects of parts whose tombstones have aged out —
// bounded garbage, reported as [Stats.Withheld] rather than deleted on a guess. Compaction removes
// parts in the low tens per merge, so this is a long window in practice.
const MaxRemovals = 4096

// Removal is a part a writer deliberately took out of the index, and the generation at which it
// did.
//
// It exists because absence is not evidence. A part missing from an index is either one the owner
// dropped — a merge consumed it, retention outlived it — or one it lost, and those are the same
// observation. Recording the removal makes the first a statement and leaves the second as what it
// is, so a replica stops treating a gap in a peer's index as an instruction to delete its own copy.
type Removal struct {
	Prefix     string
	Generation Generation
}

// Tombstone records that prefix was removed, replacing any earlier record of the same part.
func (ix *Index) Tombstone(r Removal) {
	i, found := slices.BinarySearchFunc(ix.Removed, r, compareRemovalPrefix)
	if found {
		ix.Removed[i] = r

		return
	}

	ix.Removed = slices.Insert(ix.Removed, i, r)
}

// Removals reports the set of parts this index says were removed.
func (ix *Index) Removals() map[string]struct{} {
	out := make(map[string]struct{}, len(ix.Removed))
	for _, r := range ix.Removed {
		out[r.Prefix] = struct{}{}
	}

	return out
}

// RecordsRemovals reports whether this index's writer states its removals, so absence from it can
// be read as loss rather than as a deletion.
//
// It is the same question as "was this written by a v3 writer", which the generation already
// answers: the two arrived together, and no writer stamps one without the other.
func (ix *Index) RecordsRemovals() bool { return !ix.Generation.Zero() }

// TrimRemovals drops all but the newest keep tombstones, and any naming a part the index holds
// again — a prefix cannot be both live and removed, and a live entry is the later statement.
func TrimRemovals(removals []Removal, live map[string]struct{}, keep int) []Removal {
	out := removals[:0]
	for _, r := range removals {
		if _, ok := live[r.Prefix]; !ok {
			out = append(out, r)
		}
	}

	if len(out) > keep {
		// Newest first, take keep, then restore prefix order so the encoding stays deterministic.
		slices.SortFunc(out, func(a, b Removal) int { return b.Generation.Compare(a.Generation) })
		out = out[:keep]
	}

	slices.SortFunc(out, compareRemovalPrefix)

	return out
}

func compareRemovalPrefix(a, b Removal) int {
	switch {
	case a.Prefix < b.Prefix:
		return -1
	case a.Prefix > b.Prefix:
		return 1
	default:
		return 0
	}
}

package bucketindex

import "slices"

// MaxLineage bounds the lineage catalog ([Index.Catalog]) as [MaxRemovals] bounds the tombstones, and
// for the same reason: a group's claim has to be remembered until nothing can still ask about its
// members, and no writer can know when that is. The oldest groups — lowest in block order — go first.
//
// Aging one out costs only lineage its members' own entries and wants no longer carry: a want for a
// member of a group split again can then be answered by a successor of the outer ancestry only if some
// live entry still carries the outer claim. That is where every nested want stood before the catalog
// existed, and a split is rare enough — one per straddling merge — that 4096 groups is a long history.
const MaxLineage = MaxRemovals

// RecordLineage adds to the catalog every split group's claim the index's entries and wants carry,
// and trims it to [MaxLineage]. A writer calls it on every index it commits, so a group's claim
// outlives the members that carry it: a merge that consumes a whole group folds the claim into its
// output's blocks, and a group split again leaves the outer claim on no entry at all, while a want for
// an inner member still needs it to be answered by a successor of the outer ancestry.
//
// Only a committed claim may be recorded. One allocated by a commit attempt that lost its CAS names a
// block run the retry hands out again, maybe to other parts, and recorded it would say those parts
// hold rows they do not.
func (ix *Index) RecordLineage() {
	claims := slices.Clone(ix.Catalog)

	for i := range ix.Entries {
		claims = append(claims, ix.Entries[i].Claim)
	}

	for i := range ix.Wanted {
		claims = append(claims, ix.Wanted[i].Claim)
	}

	ix.Catalog = TrimLineage(claims)
}

// MergeLineage is the catalog holding the claims of a and b, trimmed to [MaxLineage]. It is how a
// writer rebasing on a rival's index, or a replica installing a peer's, keeps what both recorded.
func MergeLineage(a, b []Claim) []Claim {
	return TrimLineage(slices.Concat(a, b))
}

// TrimLineage is claims as a stored catalog holds them: the valid ones, sorted by group, one per
// group, the newest [MaxLineage] kept. A group's run is allocated once per shard, so its bounds
// identify it, and a second claim on the same run is the same group recorded twice.
func TrimLineage(claims []Claim) []Claim {
	claims = slices.DeleteFunc(claims, func(c Claim) bool { return !c.Valid() })
	slices.SortStableFunc(claims, compareClaims)
	claims = slices.CompactFunc(claims, func(a, b Claim) bool { return compareClaims(a, b) == 0 })

	if len(claims) > MaxLineage {
		claims = slices.Delete(claims, 0, len(claims)-MaxLineage)
	}

	if len(claims) == 0 {
		return nil
	}

	return claims
}

// compareClaims orders a catalog: by group, and by ancestry within one, which only a corrupt or
// hostile input has.
func compareClaims(a, b Claim) int {
	if c := a.Group.Min.Compare(b.Group.Min); c != 0 {
		return c
	}

	if c := a.Group.Max.Compare(b.Group.Max); c != 0 {
		return c
	}

	if c := a.Blocks.Min.Compare(b.Blocks.Min); c != 0 {
		return c
	}

	return a.Blocks.Max.Compare(b.Blocks.Max)
}

// With is l and every valid claim of claims it lacks, without writing into l's backing array and
// without the [MaxLineage] bound a stored catalog has: it relates identities, it is not persisted.
func (l Lineage) With(claims ...Claim) Lineage {
	out := l

	for _, c := range claims {
		if c.Valid() && !slices.ContainsFunc(out, c.Equal) {
			if len(out) == len(l) {
				out = slices.Clip(out)
			}

			out = append(out, c)
		}
	}

	return out
}

// Lineage is every claim the index relates identities by: its catalog, and the claims its entries
// and wants carry that no commit has recorded yet.
func (ix *Index) Lineage() Lineage {
	l := Lineage(slices.Clone(ix.Catalog))

	add := func(c Claim) {
		if c.Valid() && !slices.ContainsFunc(l, c.Equal) {
			l = append(l, c)
		}
	}

	for i := range ix.Entries {
		add(ix.Entries[i].Claim)
	}

	for i := range ix.Wanted {
		add(ix.Wanted[i].Claim)
	}

	return l
}

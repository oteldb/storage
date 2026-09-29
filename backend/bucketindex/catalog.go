package bucketindex

import "slices"

// MaxLineage is how many split groups' claims the lineage catalog ([Index.Catalog]) keeps before it
// ages the oldest out, as [MaxRemovals] bounds the tombstones: no writer can know when every replica
// has seen a group, so it is remembered for a bounded number of groups instead.
//
// It is a target, not a hard cap. A claim that some outstanding want, hole or live entry still
// reaches through its ancestry is never aged out ([Index.TrimCatalog]): dropping it could turn a want
// a successor still answers into definitive absence, and three of those into a hole over rows that
// exist. Only unreachable claims go, so the catalog outgrows the target only by lineage something
// still depends on — which the live part set and the outstanding wants bound — and a split is rare
// enough, one per straddling merge, that 4096 groups is a long history.
const MaxLineage = MaxRemovals

// RecordLineage adds to the catalog every split group's claim the index's entries and wants carry,
// and trims it ([Index.TrimCatalog]). A writer calls it on every index it commits, so a group's claim
// outlives the members that carry it: a merge that consumes a whole group folds the claim into its
// output's blocks, and a group split again leaves the outer claim on no entry at all, while a want for
// an inner member still needs it to be answered by a successor of the outer ancestry.
//
// Only a committed claim may be recorded. One allocated by a commit attempt that lost its CAS names a
// block run the retry hands out again, maybe to other parts, and recorded it would say those parts
// hold rows they do not. It reports how many claims the catalog holds above [MaxLineage] because they
// are still reachable.
func (ix *Index) RecordLineage() int {
	claims := slices.Clone(ix.Catalog)

	for i := range ix.Entries {
		claims = append(claims, ix.Entries[i].Claim)
	}

	for i := range ix.Wanted {
		claims = append(claims, ix.Wanted[i].Claim)
	}

	ix.Catalog = MergeLineage(claims)

	return ix.TrimCatalog()
}

// TrimCatalog ages the oldest claims out of a catalog above [MaxLineage], but never one the index
// still reaches: a claim the ancestry of some outstanding want, hole or live entry passes through.
// It reports how many claims remain above [MaxLineage].
func (ix *Index) TrimCatalog() int {
	over := len(ix.Catalog) - MaxLineage
	if over <= 0 {
		return 0
	}

	reached := ix.reachedClaims()

	kept := ix.Catalog[:0]
	for _, c := range ix.Catalog {
		_, keep := reached[c.key()]
		if over > 0 && !keep {
			over--

			continue
		}

		kept = append(kept, c)
	}

	ix.Catalog = kept

	return max(0, len(ix.Catalog)-MaxLineage)
}

// reachedClaims is the catalog's claims the index still depends on: the whole upward ancestry of every
// want, hole and live entry — each group whose members its blocks meet, and again from that group's
// ancestors. A live entry needs its whole ancestry, not only its own claim: once it is lost, its want
// carries only that claim, and a successor of an outer ancestor answers it only through the rest.
func (ix *Index) reachedClaims() map[claimKey]struct{} {
	l := ix.Lineage()
	r := l.Relater()
	reached := make([]bool, len(l))

	for i := range ix.Wanted {
		r.reachAncestry(reached, ix.Wanted[i].Blocks)
	}

	for i := range ix.Entries {
		r.reachAncestry(reached, ix.Entries[i].Blocks)
	}

	out := make(map[claimKey]struct{})

	for i, ok := range reached {
		if ok {
			out[l[i].key()] = struct{}{}
		}
	}

	for i := range ix.Entries {
		if c := ix.Entries[i].Claim; c.Valid() {
			out[c.key()] = struct{}{}
		}
	}

	for i := range ix.Wanted {
		if c := ix.Wanted[i].Claim; c.Valid() {
			out[c.key()] = struct{}{}
		}
	}

	return out
}

// MergeLineage is the catalog holding every valid claim of the given ones, in its one canonical form:
// sorted by group, one claim per group. A group's run is allocated once per shard, so its bounds
// identify it, and a second claim on the same run is the same group recorded twice. It does not trim:
// only a writer with the whole index ([Index.TrimCatalog]) knows which claims are still reached.
func MergeLineage(catalogs ...[]Claim) []Claim {
	claims := slices.DeleteFunc(slices.Concat(catalogs...), func(c Claim) bool { return !c.Valid() })
	slices.SortStableFunc(claims, compareClaims)
	claims = slices.CompactFunc(claims, func(a, b Claim) bool { return compareClaims(a, b) == 0 })

	if len(claims) == 0 {
		return nil
	}

	return claims
}

// claimKey is the identity [MergeLineage] deduplicates by.
type claimKey struct{ group, blocks [2]Block }

func (c Claim) key() claimKey {
	return claimKey{group: [2]Block{c.Group.Min, c.Group.Max}, blocks: [2]Block{c.Blocks.Min, c.Blocks.Max}}
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
// without the [MaxLineage] target a stored catalog has: it relates identities, it is not persisted.
func (l Lineage) With(claims ...Claim) Lineage {
	if len(claims) == 0 {
		return l
	}

	known := make(map[claimKey]struct{}, len(l)+len(claims))
	for _, c := range l {
		known[c.key()] = struct{}{}
	}

	out := l

	for _, c := range claims {
		if !c.Valid() {
			continue
		}

		if _, ok := known[c.key()]; ok {
			continue
		}

		known[c.key()] = struct{}{}

		if len(out) == len(l) {
			out = slices.Clip(out)
		}

		out = append(out, c)
	}

	return out
}

// Lineage is every claim the index relates identities by: its catalog, and the claims its entries
// and wants carry that no commit has recorded yet.
func (ix *Index) Lineage() Lineage {
	l := Lineage(slices.Clone(ix.Catalog))
	sorted := len(l)

	add := func(c Claim) {
		if !c.Valid() {
			return
		}

		// The catalog is sorted, so a claim it holds is found by search; the few claims entries and
		// wants add beyond it are checked one by one.
		if _, found := slices.BinarySearchFunc(l[:sorted], c, compareClaims); found ||
			slices.ContainsFunc(l[sorted:], c.Equal) {
			return
		}

		l = append(l, c)
	}

	for i := range ix.Entries {
		add(ix.Entries[i].Claim)
	}

	for i := range ix.Wanted {
		add(ix.Wanted[i].Claim)
	}

	return l
}

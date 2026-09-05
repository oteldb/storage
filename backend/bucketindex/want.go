package bucketindex

import "slices"

// MaxWants bounds how many outstanding repair obligations an index carries, for the same reason
// [MaxRemovals] bounds tombstones: the index is a small object rewritten on every commit, and an
// unbounded list on a badly damaged node would grow it without limit.
//
// It is deliberately the same number. Past [MaxRemovals] a node can no longer tell a removed part
// from a lost one and has to adopt a peer's current index wholesale rather than repair part by
// part; a node with that many outstanding wants is in the same regime, so one constant governs
// both boundaries.
//
// Unlike a trimmed tombstone, a forgotten want is a repair that will never happen. [TrimWants]
// therefore hands back what it dropped instead of discarding it silently: exceeding the bound is
// the signal to escalate to a wholesale reseed, not a license to lose the obligation.
const MaxWants = MaxRemovals

// Want is a part this writer holds in its index but cannot read, and the generation at which it
// discovered that. It carries the whole of the lost entry's identity, because repair has to hand
// that identity back: either as the part fetched from a peer, or — when no owner has it — as the
// hole committed in its place (see [Index.RecordHole]).
//
// Blocks is the interval the missing part covered: any part containing it at a higher level
// satisfies the want, because the data is inside that successor (see [Index.Satisfying]). It is
// unset for a part written before format v5, which carries no interval; such a want is satisfiable
// only by the exact prefix.
//
// A want is an obligation with a completion condition, which is why it is not a [Removal]: a
// removal is terminal, and conflating them would make "am I repaired?" unanswerable.
type Want struct {
	Prefix           string
	Blocks           Interval
	Level            uint32
	MinTime, MaxTime int64
	Generation       Generation
}

// WantOf is the repair obligation a lost entry owes, discovered at generation g.
func WantOf(e Entry, g Generation) Want {
	return Want{
		Prefix: e.Prefix, Blocks: e.Blocks, Level: e.Level,
		MinTime: e.MinTime, MaxTime: e.MaxTime, Generation: g,
	}
}

// Entry is the index entry the want owes: the identity of the part that went missing.
func (w Want) Entry() Entry {
	return Entry{
		Prefix: w.Prefix, MinTime: w.MinTime, MaxTime: w.MaxTime,
		Blocks: w.Blocks, Level: w.Level,
	}
}

// RecordWant records that prefix is missing and must be repaired, replacing any earlier want for
// the same part. It is written by the same commit that drops the part from [Index.Entries]: a part
// leaves Entries only into Removed or into Wanted.
func (ix *Index) RecordWant(w Want) {
	i, found := slices.BinarySearchFunc(ix.Wanted, w, compareWantPrefix)
	if found {
		ix.Wanted[i] = w

		return
	}

	ix.Wanted = slices.Insert(ix.Wanted, i, w)
}

// SatisfyWant drops the want naming prefix, reporting whether one was outstanding. It is the
// completion condition: the part, or a successor containing it, is back in the index.
func (ix *Index) SatisfyWant(prefix string) bool {
	i, found := slices.BinarySearchFunc(ix.Wanted, Want{Prefix: prefix}, compareWantPrefix)
	if !found {
		return false
	}

	ix.Wanted = slices.Delete(ix.Wanted, i, i+1)

	return true
}

// Wants reports the outstanding repair obligations by prefix. Unlike [Index.Removals] the value
// carries the whole want: repair needs the block interval to accept a merged successor.
func (ix *Index) Wants() map[string]Want {
	out := make(map[string]Want, len(ix.Wanted))
	for _, w := range ix.Wanted {
		out[w.Prefix] = w
	}

	return out
}

// TrimWants drops wants already discharged — those naming a part the index holds again, or one a
// live part supersedes — and bounds what remains to keep entries, returning the kept wants and the
// ones the bound forced out.
//
// Oldest wants are kept: the longest-outstanding obligation is the one repair has had the most
// chances to discharge, so its survival is what tells an operator repair is stuck. Dropped wants
// are returned rather than discarded, because losing one silently is losing a repair.
func TrimWants(wants []Want, live []Entry, keep int) (kept, dropped []Want) {
	ix := Index{Entries: live}

	out := wants[:0]
	for _, w := range wants {
		if _, ok := ix.Discharging(w); ok {
			continue
		}

		out = append(out, w)
	}

	if len(out) > keep {
		// Oldest first, take keep, then restore prefix order so the encoding stays deterministic.
		slices.SortFunc(out, func(a, b Want) int { return a.Generation.Compare(b.Generation) })
		dropped = slices.Clone(out[keep:])
		out = out[:keep]
	}

	slices.SortFunc(out, compareWantPrefix)
	slices.SortFunc(dropped, compareWantPrefix)

	return out, dropped
}

func compareWantPrefix(a, b Want) int {
	switch {
	case a.Prefix < b.Prefix:
		return -1
	case a.Prefix > b.Prefix:
		return 1
	default:
		return 0
	}
}

// Overlaps reports whether the want's lost part covers any of [start, end] — the read-side question,
// since a query outside a want's range is answerable in full despite it.
//
// A want whose time range is entirely unset names a part of unknown extent, so it covers everything:
// a want is a claim of ignorance and errs wide.
func (w Want) Overlaps(start, end int64) bool {
	if w.MinTime == 0 && w.MaxTime == 0 {
		return true
	}

	return w.MinTime <= end && start <= w.MaxTime
}

// WantsOverlap reports whether any of wants covers [start, end].
func WantsOverlap(wants []Want, start, end int64) bool {
	for _, w := range wants {
		if w.Overlaps(start, end) {
			return true
		}
	}

	return false
}

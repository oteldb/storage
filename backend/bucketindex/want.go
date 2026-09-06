package bucketindex

import "slices"

// MaxWants is the horizon past which part-by-part repair is no longer the tool: a node owing more
// than this many parts has to adopt a peer's current index wholesale. It is deliberately
// [MaxRemovals], because past that many tombstones a node can no longer tell a removed part from a
// lost one, which is the same regime; one constant governs both boundaries.
//
// It measures how many parts are gone from this node's disk, not how long it was away. A want is
// minted per index entry whose objects will not open, so [Index.Wanted] is bounded by the shard's
// live part count — which compaction holds low and roughly logarithmic in volume: 35 live parts at
// 58 MiB of logical ingest and 85 at 234 MiB, one per ~1.67 MiB, four times the data giving 2.4
// times the parts. Reaching 4096 takes a shard of tens of TiB or years of retention. A node that
// was merely absent does not approach it from the other direction either: cluster/partsync copies
// the objects a superseding peer's index names and installs that index, so a returning node's wants
// are what that mirror has not restored yet — bounded by the same live part set and cleared by the
// next refresh, not growing with the length of the absence.
//
// Unlike [MaxRemovals] it bounds nothing. A tombstone is a fact about the past that accumulates for
// as long as the shard lives, so the list has to be cut; a want is the only record that a part is
// owed, and dropping one is dropping the repair. Nor does cutting the list buy the index anything:
// a want costs the bytes the entry it replaced did, so [Index.Wanted] is bounded by the parts the
// node has held, not by how long it has run. Exceeding the horizon is a signal to escalate, and
// the wants stay in the index until the escalation exists.
const MaxWants = MaxRemovals

// Want is a part this writer holds in its index but cannot read, and the generation at which it
// discovered that. It carries the whole of the lost entry's identity, because repair has to hand
// that identity back: either as the part fetched from a peer, or — when no owner has it — as the
// hole committed in its place (see [Index.RecordHole]).
//
// Blocks is the set of blocks the missing part covered: any part containing it at a higher level
// satisfies the want, because the data is inside that successor, and so does a complete split
// group whose claim covers it (see [Index.Satisfying]). It is unset for a part written before
// format v5, which carries no interval; such a want is satisfiable only by the exact prefix.
//
// A want is an obligation with a completion condition, which is why it is not a [Removal]: a
// removal is terminal, and conflating them would make "am I repaired?" unanswerable.
type Want struct {
	Prefix           string
	Blocks           Interval
	Claim            Claim
	Level            uint32
	MinTime, MaxTime int64
	Generation       Generation
}

// WantOf is the repair obligation a lost entry owes, discovered at generation g.
func WantOf(e Entry, g Generation) Want {
	return Want{
		Prefix: e.Prefix, Blocks: e.Blocks, Claim: e.Claim, Level: e.Level,
		MinTime: e.MinTime, MaxTime: e.MaxTime, Generation: g,
	}
}

// Entry is the index entry the want owes: the identity of the part that went missing.
func (w Want) Entry() Entry {
	return Entry{
		Prefix: w.Prefix, MinTime: w.MinTime, MaxTime: w.MaxTime,
		Blocks: w.Blocks, Claim: w.Claim, Level: w.Level,
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
	for i := range ix.Wanted {
		out[ix.Wanted[i].Prefix] = ix.Wanted[i]
	}

	return out
}

// TrimWants drops the wants already discharged — those naming a part the index holds again, or one
// a live part supersedes — and nothing else: an outstanding want is the only record that a part is
// owed, so no count trims it (see [MaxWants]).
func TrimWants(wants []Want, live []Entry) []Want {
	ix := Index{Entries: live}

	out := wants[:0]
	for i := range wants {
		w := &wants[i]
		if _, ok := ix.Discharging(*w); ok {
			continue
		}

		out = append(out, *w)
	}

	slices.SortFunc(out, compareWantPrefix)

	return out
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
	for i := range wants {
		if w := &wants[i]; w.Overlaps(start, end) {
			return true
		}
	}

	return false
}

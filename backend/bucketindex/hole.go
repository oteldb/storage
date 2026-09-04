package bucketindex

// WantOutcome says what an attempt to repair a [Want] concluded when it brought no part back. The
// distinction is the whole of the safety argument for acknowledging a loss: "no owner has the
// data" is evidence, and "I did not manage to ask every owner" never is.
//
// It is defined here, beside [Want], because it is the answer to one — the cluster layer that
// produces it and the engines that consume it must not each have their own idea of what absence
// means.
type WantOutcome uint8

const (
	// WantSatisfied means a part discharging the want is now local, and the returned entry names it.
	WantSatisfied WantOutcome = iota
	// WantAbsent means every owner the shard is expected to have answered, and none of them named a
	// part satisfying the want. It is the only outcome that can lead to a hole.
	WantAbsent
	// WantIncomplete means every peer asked answered, but they were not the whole expected owner
	// set — an owner is restarting, or its address has not resolved. Absence over a subset of the
	// owners is not absence.
	WantIncomplete
)

// RecordHole acknowledges that w cannot be repaired: it adds a hole at the lost part's identity,
// discharges the want, and raises [Index.LostParts]. The hole is what lets the shard go on — the
// obligation stops blocking — without the loss becoming invisible.
//
// It is one index mutation, so the three happen in the single CAS commit that publishes the index:
// a hole without its want discharged would be repaired forever, and a want discharged without its
// hole would be silent loss.
func (ix *Index) RecordHole(w Want) Entry {
	hole := w.Entry()
	hole.Hole = true

	ix.Add(hole)
	ix.SatisfyWant(w.Prefix)
	ix.LostParts++

	return hole
}

// Holes returns the acknowledged losses this index carries, in index order.
func (ix *Index) Holes() []Entry {
	var out []Entry

	for _, e := range ix.Entries {
		if e.Hole {
			out = append(out, e)
		}
	}

	return out
}

// Revokes reports whether e replaces the hole h: the part turned up after all, either at the exact
// identity the hole stands in for or inside a successor that subsumes it.
//
// A hole is revocable because the commit that created it is not cross-replica atomic — unlike
// ClickHouse, which checks non-existence on every replica in one transaction, an owner here can
// acknowledge a loss while a peer still holds the data. So a hole must never block the real part:
// it is replaced by it.
func Revokes(e, h Entry) bool {
	if !e.Data() || !h.Hole {
		return false
	}

	return e.Prefix == h.Prefix || e.Supersedes(h)
}

// TrimHoles drops the holes that live revokes, returning what remains. It runs on every commit, so
// any path that brings the data back — a repair fetch, a peer's entry adopted under CAS, a merge —
// revokes the hole as a side effect of committing the part.
func TrimHoles(holes, live []Entry) []Entry {
	out := holes[:0]

	for _, h := range holes {
		revoked := false

		for i := range live {
			if Revokes(live[i], h) {
				revoked = true

				break
			}
		}

		if !revoked {
			out = append(out, h)
		}
	}

	return out
}

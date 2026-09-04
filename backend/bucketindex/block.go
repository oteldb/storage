package bucketindex

// Interval is an inclusive range of block numbers, the identity a part carries alongside its
// prefix. A flush writes [n, n]; a merge over parts spanning [a … b] writes [a, b].
//
// Block numbers start at 1, so the zero value is *unset* rather than a range covering block 0.
// That distinction is what keeps a part written before format v5 — which carries no interval —
// from accidentally containing, or being contained by, anything: see [Interval.Valid].
type Interval struct {
	Min uint64
	Max uint64
}

// Valid reports whether the interval names a real range of blocks: numbering starts at 1 and the
// bounds are ordered. Everything else — the zero value of a pre-v5 entry, and any inversion a
// corrupt or hostile encoding could produce — is unset, and takes part in no containment.
func (iv Interval) Valid() bool { return iv.Min >= 1 && iv.Min <= iv.Max }

// Contains reports whether iv covers every block o covers. Both intervals must be valid: an unset
// interval neither contains nor is contained, which is the fallback to exact-prefix matching for
// parts written before v5.
func (iv Interval) Contains(o Interval) bool {
	return iv.Valid() && o.Valid() && iv.Min <= o.Min && iv.Max >= o.Max
}

// Len reports how many blocks the interval covers, 0 if unset. It orders candidate successors
// by how much of the shard they subsume.
func (iv Interval) Len() uint64 {
	if !iv.Valid() {
		return 0
	}

	return iv.Max - iv.Min + 1
}

// Supersedes reports whether e's data wholly subsumes o's: e covers every block o covers, and sits
// at a higher merge level. It is decidable from identity alone — no index comparison, no
// bookkeeping — which is what lets a repair terminate: by the time a want is serviced the data may
// exist only inside a merged successor, and that successor has to count as satisfaction.
//
// A part written before format v5 carries no interval, so it neither supersedes nor is superseded
// by anything; wants naming it fall back to exact-prefix matching until a merge rewrites it with
// an interval.
func (e Entry) Supersedes(o Entry) bool {
	return e.Blocks.Contains(o.Blocks) && e.Level > o.Level
}

// NextBlock returns the block number the next part committed to this index takes: one above the
// highest block any part it knows of covers, and 1 for an index that knows of none.
//
// Allocation is the shard owner's alone, out of its own index, and is claimed by the same CAS
// commit that adds the part — so it needs no coordination and works with the cluster layer
// absent. Two writers racing a handoff resolve through that CAS: one commit lands, and the loser
// re-reads and re-allocates above the winner.
//
// Outstanding wants count: the part they name may yet be repaired back into the index, and reusing
// its blocks would make two different parts claim the same identity.
func (ix *Index) NextBlock() uint64 {
	var maxBlock uint64
	for i := range ix.Entries {
		if b := ix.Entries[i].Blocks; b.Valid() && b.Max > maxBlock {
			maxBlock = b.Max
		}
	}

	for i := range ix.Wanted {
		if b := ix.Wanted[i].Blocks; b.Valid() && b.Max > maxBlock {
			maxBlock = b.Max
		}
	}

	return maxBlock + 1
}

// Satisfying returns the entry in this index that discharges w, if any: the part itself if the
// index still holds it, otherwise the largest part containing every block w covers. Repair asks
// this to decide whether a want is already met, and which part to fetch when it is not.
//
// "Largest" is widest interval first, then highest level, then prefix, so the answer does not
// depend on index order and a caller fetches the fewest objects for the most data.
func (ix *Index) Satisfying(w Want) (Entry, bool) {
	var (
		best  Entry
		found bool
	)

	for i := range ix.Entries {
		e := ix.Entries[i]
		if e.Prefix == w.Prefix {
			return e, true
		}

		if !e.Blocks.Contains(w.Blocks) {
			continue
		}

		if !found || betterSuccessor(e, best) {
			best, found = e, true
		}
	}

	return best, found
}

func betterSuccessor(a, b Entry) bool {
	switch {
	case a.Blocks.Len() != b.Blocks.Len():
		return a.Blocks.Len() > b.Blocks.Len()
	case a.Level != b.Level:
		return a.Level > b.Level
	default:
		return a.Prefix < b.Prefix
	}
}

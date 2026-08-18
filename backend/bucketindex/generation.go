package bucketindex

// Generation orders index *states*, which neither the part names nor [Index.FlushedEpoch] can.
//
// Both of those are high-water marks of creation: a rewrite that only removes parts — retention,
// a merge whose output was empty — moves neither, so a shrunk index is indistinguishable from one
// whose parts were silently lost. Replication needs that distinction, because it is what says
// whether a peer's index may be taken as an instruction to delete (see cluster/partsync).
//
// Term is the writer's ownership term and Counter its own write count within that term, compared
// in that order. The split is what makes the ordering survive a restore: a purely local counter
// is safe but not live, since a node restored from an older snapshot resumes below the counter
// its replicas already hold and is never adopted again. Reacquiring the shard raises the term
// instead, so the restored node supersedes on its next write, while a node that *lost* the shard
// keeps a lower term and is fenced out.
type Generation struct {
	Term    uint64
	Counter uint64
}

// Compare orders two generations, as [cmp.Compare] does.
func (g Generation) Compare(o Generation) int {
	switch {
	case g.Term != o.Term:
		if g.Term > o.Term {
			return 1
		}

		return -1
	case g.Counter != o.Counter:
		if g.Counter > o.Counter {
			return 1
		}

		return -1
	default:
		return 0
	}
}

// Zero reports a generation no writer produced: an index written before the format carried one.
// It orders below every real generation, so the first write that does carry one supersedes it.
func (g Generation) Zero() bool { return g == Generation{} }

// Next returns the generation of this writer's next index write within term.
//
// A term above the current one restarts the counter — it is a different writer's sequence, and
// the term already orders it above everything the previous one wrote. A term below is a writer
// that has been superseded and does not know it yet; its own counter still advances so that its
// local state stays monotonic, and the term keeps its writes from being adopted anywhere.
func (g Generation) Next(term uint64) Generation {
	if term > g.Term {
		return Generation{Term: term, Counter: 1}
	}

	return Generation{Term: g.Term, Counter: g.Counter + 1}
}

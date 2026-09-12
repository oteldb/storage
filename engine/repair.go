package engine

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// repairFetchesPerCycle bounds how many wants one merge cycle tries to repair. The
// [PartFetcher] bounds how many part copies that cycle runs concurrently.
//
// A repair fetch copies a whole part — up to Config.MaxPartBytes — from a peer, so an unbounded
// pass on a badly damaged node would spend the entire maintenance cycle in the network and never
// compact. The number is small on purpose: repair is anti-entropy, it runs every cycle, and a
// shard that needs more than a handful of parts back is past what part-by-part repair is for.
//
// The fetching side is not the side that needs the real budget. Under pull, every recovering node
// converges on whichever peers hold the data, so it is the *serving* side that turns one node's
// disk failure into a shard-wide degradation; capping it is future work.
const repairFetchesPerCycle = 4

// holeConfirmations is how many consecutive repair attempts must reach the same definitive-absence
// conclusion before the loss is acknowledged with a hole.
//
// One pass is a snapshot, not evidence. A peer that is up, in the ring, and has not yet finished
// loading its bucket index answers "no such part" truthfully and prematurely; so does one caught
// between the two writes of a part commit. Requiring the conclusion to repeat costs a few
// maintenance cycles of an outstanding want — a visible, recoverable state — and buys out the
// entire class of momentarily-wrong views.
const holeConfirmations = 3

// RepairStats counts what repair has done over this engine's lifetime. A want that no peer can
// satisfy stays outstanding until the loss is acknowledged, so Unsatisfiable climbing while Lost
// does not is the signal that repair is stuck rather than idle.
type RepairStats struct {
	// Local is the wants discharged with no network call, because this engine's own index gained a
	// part containing them.
	Local int64
	// Fetched is the parts pulled from a peer to discharge a want.
	Fetched int64
	// Unsatisfiable is the attempts that ended with no peer holding the part or any successor of
	// it — definitive absence, and an unrepaired shard.
	Unsatisfiable int64
	// Incomplete is the attempts that found nothing but could not have found everything: the peers
	// asked were a strict subset of the shard's expected owners, so the want stays outstanding and
	// no evidence of loss accrues.
	Incomplete int64
	// Failed is the attempts that ended in a transient failure (an unreachable peer, a copy that
	// did not finish); the want is retried on the next merge.
	Failed int64
	// Lost is the wants converted into a hole because no owner could supply the part. It is this
	// node's view of the index's monotone data-loss counter (see [Engine.LostParts]).
	Lost int64
	// Revoked is the holes replaced by the real part turning up after all.
	Revoked int64
}

// RepairStats returns a snapshot of what repair has done.
func (e *Engine) RepairStats() RepairStats {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.repaired
}

// LostParts is the shard's monotone data-loss count: the holes its writers have ever committed.
// It lives in the bucket index, so it survives a restart and every owner reads the same number.
func (e *Engine) LostParts() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.lostParts
}

// Holes returns the acknowledged losses this engine's index carries, in prefix order.
func (e *Engine) Holes() []bucketindex.Entry {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.holes)
}

// repairTarget is one part repair will try to make local this cycle: an outstanding want, or the
// identity a hole stands in for — a hole is revocable, so it is re-attempted like a want.
type repairTarget struct {
	want bucketindex.Want
	hole bool
	// sibling marks a target derived from a split group rather than read from the index: it names
	// blocks, not a prefix, and nothing about it is ever committed. See [siblingTargets].
	sibling bool
}

// repairResult is what one target's fetch concluded.
type repairResult struct {
	repairTarget

	entry   bucketindex.Entry
	outcome bucketindex.WantOutcome
	// opened is the part when this node's own backend held it; nil for one a peer supplied.
	opened *part
}

// uncount reverses what a satisfied target added to the stats when its part will not open.
func (r *repairResult) uncount(s *RepairStats) {
	switch {
	case r.hole:
		s.Revoked--
	case r.opened != nil:
		s.Local--
	default:
		s.Fetched--
	}

	s.Failed++
}

// repairWants discharges what it can of this engine's outstanding repair obligations before the
// merge that called it plans its work, so a part pulled back joins the same compaction cycle. It
// also re-attempts the holes already committed, because a hole is an acknowledgement, not a
// decision: an owner that later finds the part replaces it.
//
// It never fails the merge: a shard that cannot be repaired must still compact, so every outcome
// short of success leaves the want in the index and is counted.
func (e *Engine) repairWants(ctx context.Context) {
	if e.cfg.Backend == nil {
		return
	}

	select {
	case e.repairGate <- struct{}{}:
	case <-ctx.Done():
		return
	}

	defer func() { <-e.repairGate }()

	e.mu.Lock()
	// Pending wants are obligations too: a load that could not commit them left them for the first
	// commit this engine makes, and the repair commit is one.
	wants := slices.Concat(e.wants, e.pendingWants, e.adoptedWants)
	holes := slices.Clone(e.holes)
	entries := e.entriesLocked()
	e.mu.Unlock()

	if len(wants) == 0 && len(holes) == 0 {
		return
	}

	// A want the local index already covers is discharged by the commit below and never reaches a
	// peer: by the time repair runs, this engine's own merges may have produced the successor. The
	// same test over a hole means the data came back on its own, and the commit revokes it.
	ix := bucketindex.Index{Entries: entries}
	pending := make([]repairTarget, 0, len(wants)+len(holes))

	var stats RepairStats

	for i := range wants {
		w := &wants[i]
		if _, ok := ix.Satisfying(*w); ok {
			stats.Local++

			continue
		}

		pending = append(pending, repairTarget{want: *w})
	}

	for i := range holes {
		w := bucketindex.WantOf(holes[i], bucketindex.Generation{})
		if _, ok := ix.Satisfying(w); ok {
			stats.Revoked++

			continue
		}

		pending = append(pending, repairTarget{want: w, hole: true})
	}

	held, remote := e.openHeld(ctx, pending, &stats)

	results, failed, fetchStats := e.fetchWants(ctx, remote)
	stats.add(fetchStats)

	results = append(results, held...)

	// A want answered by a split group is answered with one member of it, and the rest of the group
	// has to arrive in the same commit: a fragment landing beside the ancestors it partly duplicates,
	// with nothing yet able to retire them, would have those rows read twice. The group's other
	// members are only knowable once one of them is in hand, which is why this is a second round.
	if extra := siblingTargets(&ix, wants, results); len(extra) > 0 {
		more, _, extraStats := e.fetchWants(ctx, extra)
		stats.add(extraStats)

		results = append(results, dropIncompleteGroups(&ix, results, more)...)
	}

	// Deterministic order so the parts are opened, and any supersession applied, the same way on
	// every node.
	slices.SortFunc(results, func(a, b repairResult) int { return strings.Compare(a.want.Prefix, b.want.Prefix) })

	lost := e.confirmLost(wants, results, failed)
	stats.Lost = int64(len(lost))

	e.publishRepaired(ctx, results, lost, stats)
	e.observeRepair(ctx, stats)
}

// observeRepair publishes the pass to the injected metrics catalog. Lost is a monotone counter
// there as it is in the index: a hole is a fact about the shard, not a level that clears.
func (e *Engine) observeRepair(ctx context.Context, s RepairStats) {
	o := e.cfg.Obs.Repair

	o.Record(ctx, s.Local, "local")
	o.Record(ctx, s.Fetched, "fetched")
	o.Record(ctx, s.Unsatisfiable, "absent")
	o.Record(ctx, s.Incomplete, "incomplete")
	o.Record(ctx, s.Failed, "failed")
	o.Lost(ctx, s.Lost)
	o.Revoked(ctx, s.Revoked)
}

// confirmLost advances the per-want evidence that a part is gone and returns the wants that have
// earned a hole. Two independent conditions must hold, because a hole over live data is worse than
// an outstanding want in every way an operator cares about.
//
// The fetch must have reached every owner the shard is expected to have — [bucketindex.WantAbsent], never
// [bucketindex.WantIncomplete]. The peer list repair is handed is whoever the cluster layer could offer, and
// during a rolling restart that is a strict subset of the owners: two reachable peers lacking the
// part says nothing about a third that holds it and is merely restarting.
//
// And the same conclusion must repeat over [holeConfirmations] consecutive attempts. A failed
// attempt, named in failed, concludes nothing but still breaks the run. Evidence lives only in
// memory, so a restart forgets it and repair has to earn it again — the bias is deliberately
// toward leaving the want outstanding.
func (e *Engine) confirmLost(
	wants []bucketindex.Want, results []repairResult, failed []string,
) []bucketindex.Want {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.holeEvidence == nil {
		e.holeEvidence = make(map[string]int)
	}

	outstanding := make(map[string]struct{}, len(wants))
	for i := range wants {
		outstanding[wants[i].Prefix] = struct{}{}
	}

	maps.DeleteFunc(e.holeEvidence, func(prefix string, _ int) bool {
		_, ok := outstanding[prefix]

		return !ok
	})

	for _, prefix := range failed {
		delete(e.holeEvidence, prefix)
	}

	var lost []bucketindex.Want

	for i := range results {
		r := &results[i]
		if r.hole || r.sibling {
			continue
		}

		if r.outcome != bucketindex.WantAbsent {
			delete(e.holeEvidence, r.want.Prefix)

			continue
		}

		e.holeEvidence[r.want.Prefix]++
		if e.holeEvidence[r.want.Prefix] >= holeConfirmations {
			lost = append(lost, r.want)
			delete(e.holeEvidence, r.want.Prefix)
		}
	}

	return lost
}

// entriesLocked is this engine's part set as index entries — its own parts plus the ones it
// adopted from a rival writer — which is what a want is tested for satisfaction against. Holes are
// not in it: an acknowledged loss holds no data, so it satisfies nothing.
// Caller holds e.mu.
func (e *Engine) entriesLocked() []bucketindex.Entry {
	out := make([]bucketindex.Entry, 0, len(e.parts)+len(e.foreign))
	for _, p := range e.parts {
		out = append(out, bucketindex.Entry{
			Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
			Blocks: p.blocks, Claim: p.claim, Level: p.level,
		})
	}

	return append(out, e.foreign...)
}

// openHeld splits the targets into those whose part this node's own backend still holds and those
// to ask peers for. The index said the part was gone, so the disk is asked before any peer: the
// index this engine loaded may be a peer's copy that knows nothing of this disk. Holding is proven
// by opening the part — surviving objects under the prefix are not a readable part.
func (e *Engine) openHeld(
	ctx context.Context, pending []repairTarget, stats *RepairStats,
) (held []repairResult, remote []repairTarget) {
	for i := range pending {
		t := &pending[i]
		if t.sibling {
			remote = append(remote, *t)

			continue
		}

		p, err := openPart(ctx, e.cfg.Backend, t.want.Prefix)
		if err != nil {
			remote = append(remote, *t)

			continue
		}

		if t.hole {
			stats.Revoked++
		} else {
			stats.Local++
		}

		held = append(held, repairResult{
			repairTarget: *t, entry: t.want.Entry(), outcome: bucketindex.WantSatisfied, opened: p,
		})
	}

	return held, remote
}

// fetchWants pulls up to [repairFetchesPerCycle] targets' parts from peers in one call — the
// fetcher answers the whole cycle at once so it can read each peer's index once and copy a part
// discharging several wants once — and returns what each attempt concluded. A failed attempt
// concludes nothing, so it is never a result; failed names the wants it was made for.
func (e *Engine) fetchWants(
	ctx context.Context, pending []repairTarget,
) ([]repairResult, []string, RepairStats) {
	var stats RepairStats

	if e.cfg.Repair == nil || len(pending) == 0 {
		return nil, nil, stats
	}

	pending = pending[:min(len(pending), repairFetchesPerCycle)]

	wants := make([]bucketindex.Want, len(pending))
	for i := range pending {
		wants[i] = pending[i].want
	}

	results := e.cfg.Repair.FetchWants(ctx, wants)

	out := make([]repairResult, 0, len(pending))

	var failed []string

	for i := range pending {
		t := &pending[i]

		// A short answer is a broken fetcher, not evidence: count it as a transient failure so the
		// want stays outstanding.
		r := FetchResult{Err: errors.Errorf("repair fetcher returned %d results for %d wants", len(results), len(pending))}
		if i < len(results) {
			r = results[i]
		}

		if r.Err != nil {
			stats.Failed++

			if !t.hole && !t.sibling {
				failed = append(failed, t.want.Prefix)
			}

			zctx.From(ctx).Warn("repair fetch failed",
				zap.String("prefix", e.cfg.Prefix), zap.String("want", t.want.Prefix), zap.Error(r.Err))

			continue
		}

		switch r.Outcome {
		case bucketindex.WantSatisfied:
			if t.hole {
				stats.Revoked++
			} else {
				stats.Fetched++
			}
		case bucketindex.WantAbsent:
			stats.Unsatisfiable++

			zctx.From(ctx).Warn("no owner holds a part satisfying the want",
				zap.String("prefix", e.cfg.Prefix), zap.String("want", t.want.Prefix))
		case bucketindex.WantIncomplete:
			stats.Incomplete++

			zctx.From(ctx).Warn("repair could not reach every owner of the shard",
				zap.String("prefix", e.cfg.Prefix), zap.String("want", t.want.Prefix))
		}

		out = append(out, repairResult{repairTarget: *t, entry: r.Entry, outcome: r.Outcome})
	}

	return out, failed, stats
}

// publishRepaired commits the cycle's outcome in one index write: the parts that came back, and
// the holes standing in for the ones that never will. That single commit is what discharges the
// wants ([Engine.nextIndexLocked] trims a want the committed entries discharge) and what revokes
// any hole a committed part covers.
//
// Local parts a repaired one supersedes are retired in the same commit, because their rows are
// inside it — the same swap a merge publishes.
func (e *Engine) publishRepaired(
	ctx context.Context, results []repairResult, lost []bucketindex.Want, stats RepairStats,
) {
	satisfied := make([]*repairResult, 0, len(results))

	for i := range results {
		if results[i].outcome == bucketindex.WantSatisfied {
			satisfied = append(satisfied, &results[i])
		}
	}

	if len(satisfied) == 0 && len(lost) == 0 && stats.Local == 0 && stats.Revoked == 0 {
		// Nothing changed, so there is nothing to commit; a want nobody could satisfy is left in
		// the index exactly as it was, and only the counters move.
		e.mu.Lock()
		e.repaired.add(stats)
		e.mu.Unlock()

		return
	}

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.Lock()

	added := make([]*part, 0, len(satisfied))
	opened := make([]bucketindex.Entry, 0, len(satisfied))

	// Two wants are routinely answered by one merged successor, and a part opened twice would be
	// committed twice and have its rows counted twice.
	have := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		have[p.prefix] = struct{}{}
	}

	// What this commit will hold, grown as parts open. `have` answers "this exact prefix is already
	// in"; this answers the weaker and more useful question, "the want's blocks are already in" —
	// which is what decides whether anything is still owed.
	live := bucketindex.Index{Entries: e.entriesLocked()}

	for _, r := range satisfied {
		ent := r.entry
		if _, dup := have[ent.Prefix]; dup {
			continue
		}

		have[ent.Prefix] = struct{}{}

		p := r.opened
		if p == nil {
			var err error

			p, err = openPart(ctx, e.cfg.Backend, ent.Prefix)
			if err != nil {
				if _, covered := live.Satisfying(r.want); covered {
					// Nothing failed and nothing is owed: a part already in this commit contains
					// the want's blocks, so the copy this entry names has nothing left to
					// discharge and the next cycle has nothing to retry. Counting it as a
					// transient failure would report repair as stuck at the moment it converged.
					continue
				}

				// The objects are here but unreadable: the want stays, and the next cycle re-copies.
				zctx.From(ctx).Warn("repaired part is not readable",
					zap.String("prefix", ent.Prefix), zap.Error(err))

				r.uncount(&stats)

				continue
			}
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		p.blocks, p.claim, p.level = ent.Blocks, ent.Claim, ent.Level

		if _, err := e.registerPartIdentitiesLocked(ctx, ent.Prefix); err != nil {
			zctx.From(ctx).Warn("repaired part identities are not readable",
				zap.String("prefix", ent.Prefix), zap.Error(err))
		}

		added = append(added, p)
		opened = append(opened, ent)
		live.Entries = append(live.Entries, ent)
	}

	superseded := supersededBy(e.parts, opened)

	committed := e.parts
	e.parts = replaceParts(e.parts, superseded, added...)
	e.pendingHoles = lost

	for i := range lost {
		zctx.From(ctx).Error("acknowledging a lost part: no owner holds it or any successor",
			zap.String("prefix", e.cfg.Prefix), zap.String("part", lost[i].Prefix))
	}

	err := e.updateIndexLocked(ctx)
	if err != nil {
		// The commit never landed, so nothing was acknowledged: the want stays outstanding and the
		// evidence for it has to be earned again.
		e.parts, e.pendingHoles = committed, nil
		stats.Lost = 0
	} else if len(superseded) > 0 {
		retired := make([]*part, 0, len(superseded))
		for _, p := range committed {
			if _, ok := superseded[p.prefix]; ok {
				retired = append(retired, p)
			}
		}

		e.retireLocked(retired)
	}

	e.repaired.add(stats)
	e.mu.Unlock()

	if err != nil {
		zctx.From(ctx).Error("repair commit failed",
			zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return
	}

	e.reclaimRetired(ctx)
}

// supersededBy is the set of live parts whose rows are wholly inside the repaired entries: a peer
// answering a want with a merged successor hands back a part containing them, and a completed split
// group jointly holds everything its claim names. Keeping both would count their records twice.
func supersededBy(parts []*part, fetched []bucketindex.Entry) map[string]struct{} {
	live := make([]bucketindex.Entry, 0, len(parts))
	for _, p := range parts {
		live = append(live, bucketindex.Entry{
			Prefix: p.prefix, Blocks: p.blocks, Claim: p.claim, Level: p.level,
		})
	}

	return bucketindex.Subsumed(live, fetched)
}

func (s *RepairStats) add(o RepairStats) {
	s.Local += o.Local
	s.Fetched += o.Fetched
	s.Unsatisfiable += o.Unsatisfiable
	s.Incomplete += o.Incomplete
	s.Failed += o.Failed
	s.Lost += o.Lost
	s.Revoked += o.Revoked
}

// siblingTargets are the extra fetches a want answered by a split group needs: the members of that
// group this node does not hold yet.
//
// A group's claim is realized only where every member is present, so one fetch per cycle would
// never complete it — a peer names one member per want, and names the same one every time. The
// missing members are therefore asked for by block instead of by prefix, which any peer resolves
// against its own index by ordinary containment.
//
// They are targets and never wants: nothing about them reaches the index, so a cycle that fetches
// none leaves it exactly as it was, and the obligation stays the original want's.
func siblingTargets(ix *bucketindex.Index, wants []bucketindex.Want, got []repairResult) []repairTarget {
	var out []repairTarget

	seen := make(map[uint64]struct{})
	known := bucketindex.Index{Entries: slices.Concat(ix.Entries, satisfiedEntries(got))}

	for i := range wants {
		w := &wants[i]
		for _, b := range known.Missing(*w) {
			if _, dup := seen[b]; dup {
				continue
			}

			seen[b] = struct{}{}
			out = append(out, repairTarget{
				want: bucketindex.Want{
					Blocks: bucketindex.Blocks(b), MinTime: w.MinTime, MaxTime: w.MaxTime,
				},
				sibling: true,
			})
		}
	}

	return out
}

// satisfiedEntries are the parts a repair round actually made local.
func satisfiedEntries(results []repairResult) []bucketindex.Entry {
	out := make([]bucketindex.Entry, 0, len(results))
	for i := range results {
		if results[i].outcome == bucketindex.WantSatisfied {
			out = append(out, results[i].entry)
		}
	}

	return out
}

// dropIncompleteGroups discards the group members a round could not complete — the per-cycle fetch
// budget cuts a wide split short — so a partial group is never committed. Its rows overlap the
// ancestors the node still holds and nothing could retire them, so half a group is worse than none;
// the objects stay copied and the next cycle asks again.
func dropIncompleteGroups(ix *bucketindex.Index, have, results []repairResult) []repairResult {
	entries := slices.Concat(ix.Entries, satisfiedEntries(have), satisfiedEntries(results))

	out := results[:0]

	for i := range results {
		r := &results[i]
		if r.outcome == bucketindex.WantSatisfied && !r.entry.Complete(entries) {
			continue
		}

		out = append(out, *r)
	}

	return out
}

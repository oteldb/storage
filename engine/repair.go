package engine

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// repairFetchesPerCycle bounds how many wants one merge cycle tries to repair, and
// repairFetchConcurrency how many of those fetches are in flight at once.
//
// A repair fetch copies a whole part — up to Config.MaxPartBytes — from a peer, so an unbounded
// pass on a badly damaged node would spend the entire maintenance cycle in the network and never
// compact. Both numbers are small on purpose: repair is anti-entropy, it runs every cycle, and a
// shard that needs more than a handful of parts back is past what part-by-part repair is for.
//
// The fetching side is not the side that needs the real budget. Under pull, every recovering node
// converges on whichever peers hold the data, so it is the *serving* side that turns one node's
// disk failure into a shard-wide degradation; capping it is future work.
const (
	repairFetchesPerCycle  = 4
	repairFetchConcurrency = 2
)

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
}

// repairResult is what one target's fetch concluded.
type repairResult struct {
	repairTarget

	entry   bucketindex.Entry
	outcome bucketindex.WantOutcome
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

	e.mu.Lock()
	wants := slices.Clone(e.wants)
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

	for _, w := range wants {
		if _, ok := ix.Satisfying(w); ok {
			stats.Local++

			continue
		}

		pending = append(pending, repairTarget{want: w})
	}

	for _, h := range holes {
		w := bucketindex.WantOf(h, bucketindex.Generation{})
		if _, ok := ix.Satisfying(w); ok {
			stats.Revoked++

			continue
		}

		pending = append(pending, repairTarget{want: w, hole: true})
	}

	results, fetchStats := e.fetchWants(ctx, pending)
	stats.add(fetchStats)

	lost := e.confirmLost(wants, results)
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
// And the same conclusion must repeat over [holeConfirmations] attempts. Evidence lives only in
// memory, so a restart forgets it and repair has to earn it again — the bias is deliberately
// toward leaving the want outstanding.
func (e *Engine) confirmLost(wants []bucketindex.Want, results []repairResult) []bucketindex.Want {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.holeEvidence == nil {
		e.holeEvidence = make(map[string]int)
	}

	outstanding := make(map[string]struct{}, len(wants))
	for _, w := range wants {
		outstanding[w.Prefix] = struct{}{}
	}

	maps.DeleteFunc(e.holeEvidence, func(prefix string, _ int) bool {
		_, ok := outstanding[prefix]

		return !ok
	})

	var lost []bucketindex.Want

	for i := range results {
		r := &results[i]
		if r.hole {
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
			Blocks: p.blocks, Level: p.level,
		})
	}

	return append(out, e.foreign...)
}

// fetchWants pulls up to [repairFetchesPerCycle] targets' parts from peers, bounded to
// [repairFetchConcurrency] concurrent copies, and returns what each attempt concluded.
func (e *Engine) fetchWants(ctx context.Context, pending []repairTarget) ([]repairResult, RepairStats) {
	var stats RepairStats

	if e.cfg.Repair == nil || len(pending) == 0 {
		return nil, stats
	}

	pending = pending[:min(len(pending), repairFetchesPerCycle)]

	var (
		mu  sync.Mutex
		out []repairResult
		wg  sync.WaitGroup
	)

	sem := make(chan struct{}, repairFetchConcurrency)

	for _, t := range pending {
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer func() { <-sem; wg.Done() }()

			ent, outcome, err := e.cfg.Repair.FetchWant(ctx, t.want)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				stats.Failed++

				zctx.From(ctx).Warn("repair fetch failed",
					zap.String("prefix", e.cfg.Prefix), zap.String("want", t.want.Prefix), zap.Error(err))

				return
			}

			switch outcome {
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

			out = append(out, repairResult{repairTarget: t, entry: ent, outcome: outcome})
		}()
	}

	wg.Wait()

	// Deterministic order so the parts are opened, and any supersession applied, the same way on
	// every node.
	slices.SortFunc(out, func(a, b repairResult) int { return strings.Compare(a.want.Prefix, b.want.Prefix) })

	return out, stats
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
	fetched := make([]bucketindex.Entry, 0, len(results))

	for i := range results {
		if results[i].outcome == bucketindex.WantSatisfied {
			fetched = append(fetched, results[i].entry)
		}
	}

	if len(fetched) == 0 && len(lost) == 0 && stats.Local == 0 && stats.Revoked == 0 {
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

	added := make([]*part, 0, len(fetched))
	opened := make([]bucketindex.Entry, 0, len(fetched))

	// Two wants are routinely answered by one merged successor, and a part opened twice would be
	// committed twice and have its rows counted twice.
	have := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		have[p.prefix] = struct{}{}
	}

	for _, ent := range fetched {
		if _, dup := have[ent.Prefix]; dup {
			continue
		}

		have[ent.Prefix] = struct{}{}

		p, err := openPart(ctx, e.cfg.Backend, ent.Prefix)
		if err != nil {
			// The objects are here but unreadable: the want stays, and the next cycle re-copies.
			zctx.From(ctx).Warn("repaired part is not readable",
				zap.String("prefix", ent.Prefix), zap.Error(err))

			stats.Fetched--
			stats.Failed++

			continue
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		p.blocks, p.level = ent.Blocks, ent.Level

		if _, err := e.registerPartIdentitiesLocked(ctx, ent.Prefix); err != nil {
			zctx.From(ctx).Warn("repaired part identities are not readable",
				zap.String("prefix", ent.Prefix), zap.Error(err))
		}

		added = append(added, p)
		opened = append(opened, ent)
	}

	superseded := supersededBy(e.parts, opened)

	committed := e.parts
	e.parts = replaceParts(e.parts, superseded, added...)
	e.pendingHoles = lost

	for _, w := range lost {
		zctx.From(ctx).Error("acknowledging a lost part: no owner holds it or any successor",
			zap.String("prefix", e.cfg.Prefix), zap.String("part", w.Prefix))
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

// supersededBy is the set of live parts whose rows are wholly inside one of the repaired entries:
// a peer answering a want with a merged successor hands back a part containing them, and keeping
// both would count their records twice.
func supersededBy(parts []*part, fetched []bucketindex.Entry) map[string]struct{} {
	out := make(map[string]struct{})

	for _, ent := range fetched {
		for _, p := range parts {
			if p.prefix == ent.Prefix {
				continue
			}

			if ent.Supersedes(bucketindex.Entry{Prefix: p.prefix, Blocks: p.blocks, Level: p.level}) {
				out[p.prefix] = struct{}{}
			}
		}
	}

	return out
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

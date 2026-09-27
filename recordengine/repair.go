package recordengine

import (
	"context"
	"slices"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/repair"
)

// RepairStats counts what repair has done over this engine's lifetime.
type RepairStats = bucketindex.RepairStats

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

	opened := make(map[string]*part)
	pass := repair.Pass{
		Fetcher: e.cfg.Repair,
		Prefix:  e.cfg.Prefix,
		Hold: func(ctx context.Context, prefix string) bool {
			p, err := e.openRepaired(ctx, prefix)
			if err != nil {
				return false
			}

			opened[prefix] = p

			return true
		},
	}

	plan := pass.Run(ctx, entries, wants, holes)

	lost := e.confirmLost(wants, plan)
	plan.Stats.Lost = int64(len(lost))

	e.observeRepair(ctx, e.publishRepaired(ctx, plan, opened, lost))
}

func (e *Engine) openRepaired(ctx context.Context, prefix string) (*part, error) {
	return openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, e.cfg.Obs.Corruption)
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

// confirmLost advances this engine's absence evidence with the pass. The evidence lives only in
// memory, so a restart forgets it and repair has to earn it again.
func (e *Engine) confirmLost(wants []bucketindex.Want, plan repair.Plan) []bucketindex.Want {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.holeEvidence == nil {
		e.holeEvidence = make(map[string]int)
	}

	return repair.ConfirmLost(e.holeEvidence, wants, plan.Attempts, plan.Failed)
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

// publishRepaired commits the pass's outcome in one index write: the parts [repair.Admit] lets
// back in, and the holes standing in for the ones that never will. That single commit is what
// discharges the wants ([Engine.nextIndexLocked] trims a want the committed entries discharge) and
// what revokes any hole a committed part covers.
//
// Local parts a repaired one supersedes are retired in the same commit, because their rows are
// inside it — the same swap a merge publishes.
//
// It returns stats as the commit's outcome corrected them, the only value [Engine.observeRepair]
// may be given: a failed commit acknowledged no loss.
func (e *Engine) publishRepaired(
	ctx context.Context, plan repair.Plan, opened map[string]*part, lost []bucketindex.Want,
) RepairStats {
	stats := plan.Stats

	if len(plan.Units) == 0 && len(lost) == 0 && stats.Local == 0 && stats.Revoked == 0 {
		// Nothing changed, so there is nothing to commit; a want nobody could satisfy is left in
		// the index exactly as it was, and only the counters move.
		e.mu.Lock()
		e.repaired.Add(stats)
		e.mu.Unlock()

		return stats
	}

	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.Lock()

	admitted := repair.Admit(ctx, e.entriesLocked(), plan.Units, func(r *repair.Result) error {
		if r.Held {
			return nil
		}

		p, err := e.openRepaired(ctx, r.Entry.Prefix)
		if err != nil {
			return err
		}

		opened[r.Entry.Prefix] = p

		return nil
	}, &stats)

	added := make([]*part, 0, len(admitted))

	for i := range admitted {
		ent := &admitted[i]
		p := opened[ent.Prefix]
		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		p.blocks, p.claim, p.level = ent.Blocks, ent.Claim, ent.Level

		if _, err := e.registerPartIdentitiesLocked(ctx, ent.Prefix); err != nil {
			zctx.From(ctx).Warn("repaired part identities are not readable",
				zap.String("prefix", ent.Prefix), zap.Error(err))
		}

		added = append(added, p)
	}

	superseded := supersededBy(e.parts, admitted)

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

	e.repaired.Add(stats)
	e.mu.Unlock()

	if err != nil {
		zctx.From(ctx).Error("repair commit failed",
			zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return stats
	}

	e.reclaimRetired(ctx)

	return stats
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

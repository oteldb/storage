package engine

import (
	"context"
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

// RepairStats counts what repair has done over this engine's lifetime. A want that no peer can
// satisfy stays outstanding, so Unsatisfiable climbing is the signal that repair is stuck rather
// than idle.
type RepairStats struct {
	// Local is the wants discharged with no network call, because this engine's own index gained a
	// part containing them.
	Local int64
	// Fetched is the parts pulled from a peer to discharge a want.
	Fetched int64
	// Unsatisfiable is the attempts that ended with no peer holding the part or any successor of
	// it — definitive absence, and an unrepaired shard.
	Unsatisfiable int64
	// Failed is the attempts that ended in a transient failure (an unreachable peer, a copy that
	// did not finish); the want is retried on the next merge.
	Failed int64
}

// RepairStats returns a snapshot of what repair has done.
func (e *Engine) RepairStats() RepairStats {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.repaired
}

// repairWants discharges what it can of this engine's outstanding repair obligations before the
// merge that called it plans its work, so a part pulled back joins the same compaction cycle.
//
// It never fails the merge: a shard that cannot be repaired must still compact, so every outcome
// short of success leaves the want in the index and is counted.
func (e *Engine) repairWants(ctx context.Context) {
	if e.cfg.Backend == nil {
		return
	}

	e.mu.Lock()
	wants := slices.Clone(e.wants)
	entries := e.entriesLocked()
	e.mu.Unlock()

	if len(wants) == 0 {
		return
	}

	// A want the local index already covers is discharged by the commit below and never reaches a
	// peer: by the time repair runs, this engine's own merges may have produced the successor.
	ix := bucketindex.Index{Entries: entries}
	pending := make([]bucketindex.Want, 0, len(wants))

	var local int64

	for _, w := range wants {
		if _, ok := ix.Satisfying(w); ok {
			local++

			continue
		}

		pending = append(pending, w)
	}

	fetched, stats := e.fetchWants(ctx, pending)
	stats.Local = local

	e.publishRepaired(ctx, fetched, stats)
}

// entriesLocked is this engine's part set as index entries — its own parts plus the ones it
// adopted from a rival writer — which is what a want is tested for satisfaction against.
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

// fetchWants pulls up to [repairFetchesPerCycle] wants' parts from peers, bounded to
// [repairFetchConcurrency] concurrent copies, and returns the entries that came back.
func (e *Engine) fetchWants(ctx context.Context, pending []bucketindex.Want) ([]bucketindex.Entry, RepairStats) {
	var stats RepairStats

	if e.cfg.Repair == nil || len(pending) == 0 {
		return nil, stats
	}

	pending = pending[:min(len(pending), repairFetchesPerCycle)]

	var (
		mu  sync.Mutex
		out []bucketindex.Entry
		wg  sync.WaitGroup
	)

	sem := make(chan struct{}, repairFetchConcurrency)

	for _, w := range pending {
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer func() { <-sem; wg.Done() }()

			ent, ok, err := e.cfg.Repair.FetchWant(ctx, w)

			mu.Lock()
			defer mu.Unlock()

			switch {
			case err != nil:
				stats.Failed++

				zctx.From(ctx).Warn("repair fetch failed",
					zap.String("prefix", e.cfg.Prefix), zap.String("want", w.Prefix), zap.Error(err))
			case !ok:
				stats.Unsatisfiable++

				zctx.From(ctx).Warn("no peer holds a part satisfying the want",
					zap.String("prefix", e.cfg.Prefix), zap.String("want", w.Prefix))
			default:
				stats.Fetched++

				out = append(out, ent)
			}
		}()
	}

	wg.Wait()

	// Deterministic order so the parts are opened, and any supersession applied, the same way on
	// every node.
	slices.SortFunc(out, func(a, b bucketindex.Entry) int { return strings.Compare(a.Prefix, b.Prefix) })

	return out, stats
}

// publishRepaired opens the repaired parts and commits them into the index, which is what
// discharges their wants ([Engine.nextIndexLocked] trims a want the committed entries satisfy).
// Local parts a repaired one supersedes are retired in the same commit, because their rows are
// inside it — the same swap a merge publishes.
func (e *Engine) publishRepaired(ctx context.Context, fetched []bucketindex.Entry, stats RepairStats) {
	if len(fetched) == 0 && stats.Local == 0 {
		// Nothing was repaired, so there is nothing to commit; a want nobody could satisfy is left
		// in the index exactly as it was, and only the counters move.
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

	for _, ent := range fetched {
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

	err := e.updateIndexLocked(ctx)
	if err != nil {
		e.parts = committed
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
	s.Failed += o.Failed
}

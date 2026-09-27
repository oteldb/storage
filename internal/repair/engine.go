package repair

import (
	"context"
	"sync"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/obs"
)

// Host is one engine as [Drive] repairs it; P is the engine's part handle.
//
// Methods suffixed Locked run with the engine's state lock held. The commit additionally holds the
// flush lock, taken first.
type Host[P any] interface {
	// Locks are the engine's state lock and its flush lock.
	Locks() (state, flush sync.Locker)

	// ObligationsLocked are the wants and holes a pass services.
	ObligationsLocked() ([]bucketindex.Want, []bucketindex.Entry)
	// LiveLocked is the engine's own live part set, and the entries it adopted from a rival writer.
	LiveLocked() ([]P, []bucketindex.Entry)
	// Identity is p's index identity.
	Identity(p P) bucketindex.Entry
	// Open opens the part at prefix from the engine's own backend.
	Open(ctx context.Context, prefix string) (P, error)
	// AdoptLocked stamps ent's identity on a repaired part and registers its stream identities.
	AdoptLocked(ctx context.Context, p P, ent *bucketindex.Entry) error
	// SwapLocked installs parts as the live set and holes as the losses the next commit acknowledges.
	SwapLocked(parts []P, holes []bucketindex.Want)
	CommitLocked(ctx context.Context) error
	RetireLocked(parts []P)
	// Reclaim deletes retired parts' objects; it runs without the state lock.
	Reclaim(ctx context.Context)
}

// Config is what a pass needs besides its [Host].
type Config struct {
	Fetcher bucketindex.PartFetcher
	Obs     *obs.Repair
	// Prefix names the engine in logs.
	Prefix string
}

// State is what repair keeps across one engine's passes. The zero value is ready to use.
type State struct {
	once sync.Once
	// gate makes passes single-flight while honoring a waiter's ctx.
	gate chan struct{}

	// pass numbers the passes; tried is the pass each outstanding want or hole was last sent to
	// peers in. Both are touched only by the pass holding the gate.
	pass  uint64
	tried map[string]uint64

	mu       sync.Mutex
	evidence map[string]int
	stats    bucketindex.RepairStats
}

// Stats returns what repair has done over the engine's lifetime.
func (s *State) Stats() bucketindex.RepairStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stats
}

// remember records the targets plan sent to peers and forgets any no longer outstanding.
func (s *State) remember(plan Plan, wants []bucketindex.Want, holes []bucketindex.Entry) {
	outstanding := make(map[string]struct{}, len(wants)+len(holes))
	for i := range wants {
		outstanding[wants[i].Prefix] = struct{}{}
	}

	for i := range holes {
		outstanding[holes[i].Prefix] = struct{}{}
	}

	for prefix := range s.tried {
		if _, ok := outstanding[prefix]; !ok {
			delete(s.tried, prefix)
		}
	}

	if s.tried == nil {
		s.tried = make(map[string]uint64)
	}

	for _, prefix := range plan.Asked {
		s.tried[prefix] = s.pass
	}
}

func (s *State) add(st bucketindex.RepairStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.Add(st)
}

func (s *State) confirmLost(wants []bucketindex.Want, plan Plan) []bucketindex.Want {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.evidence == nil {
		s.evidence = make(map[string]int)
	}

	return ConfirmLost(s.evidence, wants, plan.Attempts, plan.Failed)
}

// Drive runs one repair pass over h's outstanding wants and holes, and commits what it brings back.
// It never fails: every outcome short of success leaves the want in the index and is counted.
func Drive[P any](ctx context.Context, s *State, cfg Config, h Host[P]) {
	s.once.Do(func() { s.gate = make(chan struct{}, 1) })

	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return
	}

	defer func() { <-s.gate }()

	state, _ := h.Locks()

	state.Lock()
	wants, holes := h.ObligationsLocked()
	live := entriesLocked(h)
	state.Unlock()

	if len(wants) == 0 && len(holes) == 0 {
		return
	}

	s.pass++

	opened := make(map[string]P)
	pass := Pass{
		Fetcher: cfg.Fetcher,
		Prefix:  cfg.Prefix,
		Tried:   s.tried,
		Hold: func(ctx context.Context, prefix string) bool {
			p, err := h.Open(ctx, prefix)
			if err != nil {
				return false
			}

			opened[prefix] = p

			return true
		},
	}

	plan := pass.Run(ctx, live, wants, holes)
	s.remember(plan, wants, holes)

	lost := s.confirmLost(wants, plan)
	plan.Stats.Lost = int64(len(lost))

	observe(ctx, cfg.Obs, publish(ctx, s, cfg, h, plan, opened, lost))
}

func entriesLocked[P any](h Host[P]) []bucketindex.Entry {
	parts, foreign := h.LiveLocked()

	out := make([]bucketindex.Entry, 0, len(parts)+len(foreign))
	for _, p := range parts {
		out = append(out, h.Identity(p))
	}

	return append(out, foreign...)
}

// observe publishes the pass to the metrics catalog. Lost is a monotone counter there as it is in
// the index: a hole is a fact about the shard, not a level that clears.
func observe(ctx context.Context, o *obs.Repair, s bucketindex.RepairStats) {
	o.Record(ctx, s.Local, "local")
	o.Record(ctx, s.Fetched, "fetched")
	o.Record(ctx, s.Unsatisfiable, "absent")
	o.Record(ctx, s.Incomplete, "incomplete")
	o.Record(ctx, s.Failed, "failed")
	o.Lost(ctx, s.Lost)
	o.Revoked(ctx, s.Revoked)
}

// publish commits the pass in one index write: the parts [Admit] lets back in, and the holes for
// the losses. That commit discharges the wants and revokes any hole a committed part covers; the
// live parts a repaired one subsumes are retired in it, the same swap a merge publishes.
//
// It returns stats as the commit corrected them: a failed commit acknowledged no loss.
func publish[P any](
	ctx context.Context, s *State, cfg Config, h Host[P], plan Plan, opened map[string]P, lost []bucketindex.Want,
) bucketindex.RepairStats {
	stats := plan.Stats

	if len(plan.Units) == 0 && len(lost) == 0 && stats.Local == 0 && stats.Revoked == 0 {
		s.add(stats)

		return stats
	}

	state, flush := h.Locks()

	flush.Lock()
	defer flush.Unlock()

	state.Lock()

	admitted := Admit(ctx, entriesLocked(h), plan.Units, func(r *Result) error {
		if r.Held {
			return nil
		}

		p, err := h.Open(ctx, r.Entry.Prefix)
		if err != nil {
			return err
		}

		opened[r.Entry.Prefix] = p

		return nil
	}, &stats)

	committed, _ := h.LiveLocked()
	own := make([]bucketindex.Entry, len(committed))

	for i, p := range committed {
		own[i] = h.Identity(p)
	}

	superseded := bucketindex.Subsumed(own, admitted)

	next := make([]P, 0, len(committed)+len(admitted))
	retired := make([]P, 0, len(superseded))

	for i, p := range committed {
		if _, ok := superseded[own[i].Prefix]; ok {
			retired = append(retired, p)
		} else {
			next = append(next, p)
		}
	}

	for i := range admitted {
		p := opened[admitted[i].Prefix]
		if err := h.AdoptLocked(ctx, p, &admitted[i]); err != nil {
			zctx.From(ctx).Warn("repaired part identities are not readable",
				zap.String("prefix", admitted[i].Prefix), zap.Error(err))
		}

		next = append(next, p)
	}

	h.SwapLocked(next, lost)

	for i := range lost {
		zctx.From(ctx).Error("acknowledging a lost part: no owner holds it or any successor",
			zap.String("prefix", cfg.Prefix), zap.String("part", lost[i].Prefix))
	}

	err := h.CommitLocked(ctx)
	if err != nil {
		// The commit never landed, so nothing was acknowledged: the want stays outstanding and the
		// evidence for it has to be earned again.
		h.SwapLocked(committed, nil)
		stats.Lost = 0
	} else if len(retired) > 0 {
		h.RetireLocked(retired)
	}

	s.add(stats)
	state.Unlock()

	if err != nil {
		zctx.From(ctx).Error("repair commit failed", zap.String("prefix", cfg.Prefix), zap.Error(err))

		return stats
	}

	h.Reclaim(ctx)

	return stats
}

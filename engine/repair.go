package engine

import (
	"context"
	"slices"
	"sync"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/repair"
)

// RepairStats counts what repair has done over this engine's lifetime.
type RepairStats = bucketindex.RepairStats

// RepairStats returns a snapshot of what repair has done.
func (e *Engine) RepairStats() RepairStats { return e.repairs.Stats() }

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

// repairWants runs a repair pass before the merge that called it plans its work, so a part pulled
// back joins the same compaction cycle.
func (e *Engine) repairWants(ctx context.Context) {
	if e.cfg.Backend == nil {
		return
	}

	repair.Drive(ctx, &e.repairs, repair.Config{Fetcher: e.cfg.Repair, Obs: e.cfg.Obs.Repair, Prefix: e.cfg.Prefix},
		repairHost{e})
}

// repairHost adapts the engine to [repair.Host].
type repairHost struct{ e *Engine }

var _ repair.Host[*part] = repairHost{}

func (h repairHost) Locks() (state, flush sync.Locker) { return &h.e.mu, &h.e.flushMu }

// ObligationsLocked includes pending wants: a load that could not commit them left them for the
// first commit this engine makes, and the repair commit is one.
func (h repairHost) ObligationsLocked() ([]bucketindex.Want, []bucketindex.Entry) {
	return slices.Concat(h.e.wants, h.e.pendingWants, h.e.adoptedWants), slices.Clone(h.e.holes)
}

func (h repairHost) LiveLocked() ([]*part, []bucketindex.Entry) { return h.e.parts, h.e.foreign }
func (h repairHost) CommitLocked(ctx context.Context) error     { return h.e.updateIndexLocked(ctx) }
func (h repairHost) RetireLocked(parts []*part)                 { h.e.retireLocked(parts) }
func (h repairHost) Reclaim(ctx context.Context)                { h.e.reclaimRetired(ctx) }

func (h repairHost) Identity(p *part) bucketindex.Entry {
	return bucketindex.Entry{
		Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime,
		Blocks: p.blocks, Claim: p.claim, Level: p.level, Term: p.term,
	}
}

func (h repairHost) Open(ctx context.Context, prefix string) (*part, error) {
	return openPart(ctx, h.e.cfg.Backend, prefix, h.e.cfg.Obs.Corruption, h.e.readCompressors)
}

func (h repairHost) AdoptLocked(ctx context.Context, p *part, ent *bucketindex.Entry) error {
	p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
	p.blocks, p.claim, p.level, p.term = ent.Blocks, ent.Claim, ent.Level, ent.Term

	_, err := h.e.registerPartIdentitiesLocked(ctx, ent.Prefix)

	return err
}

func (h repairHost) SwapLocked(parts []*part, holes []bucketindex.Want) {
	h.e.parts, h.e.pendingHoles = parts, holes
}

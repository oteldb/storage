package storage

import (
	"context"
	"maps"
	"math"

	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// Holding a shard is not the same as being able to answer for it. A node that restarts — or that a
// rebalance hands a shard to — comes back with the shard's flushed parts and an empty head, so
// everything the shard held unflushed at that moment is missing here. The read path takes one
// owner's answer as complete, so serving that engine returns a hole the caller cannot distinguish
// from real absence.
//
// Such an engine records the point past which it cannot answer — the newest data its parts hold, so
// everything after it was unflushed and is gone from here — and disclaims any query reaching past it,
// exactly as an absent shard does: [cluster.ErrShardAbsent] is a failover, so an owner that kept the
// head answers instead. Everything older is still served locally.
//
// The gap closes when the parts advance past that point, which happens only when the shard's
// compaction owner flushes: a flush drains the whole head, so a part carrying newer data than this
// node ever had carries what this node lost. A merge cannot close it by accident — merging preserves
// its inputs' time range — and the bound stays in the data's own time domain, so it does not assume
// ingest timestamps track the wall clock.
//
// A node whose WAL restored a head records no gap: that log is the record of everything it accepted
// as the shard's primary, so replaying it leaves nothing behind. An empty log is not proof of an
// empty head (a secondary's head is never logged, and an idle primary's is empty either way), so the
// gap is recorded conservatively and costs a failover per recent-window query until the next flush.
//
// An unsatisfied repair obligation — a part this node's index names but cannot read
// (a bucketindex Want) — is the same statement about a different range, and takes the same route
// through [Storage.canAnswer]. It carries the lost part's own time bounds, so only a query reaching
// into that range disclaims; everything else is answered here as usual. It ends when repair fetches
// the part back or the loss is acknowledged as a hole, which discharges the want — so failing reads
// is bounded by repair or by an operator accepting the loss, never open-ended.
//
// Disclaiming is a failover, not an answer, and that is the whole of the difference between this and
// serving short: a complete owner answers instead and the query succeeds. Only when *every* owner
// disclaims does the distinction bite — no owner holding the shard is genuine absence and reads
// empty, while an owner holding it and knowing it is short fails the read. [cluster.Disclaims] is
// where those two are told apart.

// readGap is what a shard's local engine cannot answer for: everything after `after`, the newest
// timestamp its parts held when it came back without a head. The bound is inclusive — a gap is a
// claim of ignorance, so it errs wide — and unbounded above, since nothing here knows how far past
// it the lost head reached.
type readGap struct {
	after int64
}

// overlaps reports whether a query window reaches into the gap.
func (g readGap) overlaps(_, end int64) bool { return end >= g.after }

// gapKey identifies one shard's engine: gaps are per signal, since a node can recover one signal's
// head and lose another's.
type gapKey struct {
	sig   signal.Signal
	shard signal.TenantID
}

// noteRecoveryGaps records a [readGap] for every engine that came out of startup recovery without a
// head. Called after recovery and before the node joins the cluster, so no read is served ungated.
func (s *Storage) noteRecoveryGaps() {
	for tid, eng := range s.engineSnapshotByTenant() {
		if eng.HeadSampleCount() == 0 {
			s.noteReadGap(signal.Metric, tid, metricPartsMax(eng))
		}
	}

	for sig, engines := range map[signal.Signal]map[signal.TenantID]*recordengine.Engine{
		signal.Log:     s.logEngineSnapshotByTenant(),
		signal.Trace:   s.traceEngineSnapshotByTenant(),
		signal.Profile: s.profileEngineSnapshotByTenant(),
	} {
		for tid, eng := range engines {
			if eng.HeadRecordCount() == 0 {
				s.noteReadGap(sig, tid, recordPartsMax(eng))
			}
		}
	}
}

// noteReadGap records that this node cannot answer past partsMax on the shard. A shard with no parts
// at all reports [math.MinInt64]: nothing here is known to be complete.
func (s *Storage) noteReadGap(sig signal.Signal, shard signal.TenantID, partsMax int64) {
	s.gapMu.Lock()
	defer s.gapMu.Unlock()

	if s.gaps == nil {
		s.gaps = make(map[gapKey]readGap)
	}

	s.gaps[gapKey{sig: sig, shard: shard}] = readGap{after: partsMax}
}

// hasReadGap reports whether the shard has an open gap in this signal, without a window — the seam
// builder's check, so a shard with nothing missing keeps the bare engine as its fetcher (and its
// capabilities with it).
func (s *Storage) hasReadGap(sig signal.Signal, shard signal.TenantID) bool {
	s.gapMu.RLock()
	defer s.gapMu.RUnlock()

	_, ok := s.gaps[gapKey{sig: sig, shard: shard}]

	return ok
}

// readGapOverlaps reports whether a query window falls into the shard's open gap.
func (s *Storage) readGapOverlaps(sig signal.Signal, shard signal.TenantID, start, end int64) bool {
	s.gapMu.RLock()
	defer s.gapMu.RUnlock()

	g, ok := s.gaps[gapKey{sig: sig, shard: shard}]

	return ok && g.overlaps(start, end)
}

// wantOverlaps reports whether the shard's local engine carries an unsatisfied repair obligation
// covering the window: an entry its index names, whose part it cannot read and has not got back.
func (s *Storage) wantOverlaps(sig signal.Signal, shard signal.TenantID, start, end int64) bool {
	if sig == signal.Metric {
		eng, ok := s.lookupEngine(shard)

		return ok && eng.WantOverlaps(start, end)
	}

	eng, ok := s.lookupRecordEngine(sig, shard)

	return ok && eng.WantOverlaps(start, end)
}

// hasWants reports whether the shard has any outstanding obligation in this signal, without a
// window — the seam builder's check, the [Storage.hasReadGap] twin.
func (s *Storage) hasWants(sig signal.Signal, shard signal.TenantID) bool {
	if sig == signal.Metric {
		eng, ok := s.lookupEngine(shard)

		return ok && eng.HasWants()
	}

	eng, ok := s.lookupRecordEngine(sig, shard)

	return ok && eng.HasWants()
}

// canAnswer returns the error to disclaim the shard with when this node may not serve it for
// [start, end], and nil when it may. held says whether this node has an engine for the shard at all;
// passing it through here rather than branching at the call site is what keeps the two disclaims —
// "I hold nothing" and "I hold it and it is short" — decided in one place.
//
// It meters and logs the incomplete case, which is the one an operator acts on. Two facts reach it:
// a read gap (the unflushed head this node came back without) and an unsatisfied want (a part its
// index names but it cannot read). They are the same statement — this node cannot answer completely
// for that range — and both fail over. A committed hole is neither: acknowledging a loss discharges
// its want, so reads resume.
//
// The enumeration RPCs spell "no time filter" as a zero window; widen it, since an unbounded listing
// certainly overlaps.
func (s *Storage) canAnswer(
	ctx context.Context, op string, sig signal.Signal, shard signal.TenantID, held bool, start, end int64,
) error {
	if !held {
		return cluster.ErrShardAbsent
	}

	if start == 0 && end == 0 {
		start, end = math.MinInt64, math.MaxInt64
	}

	var reason string

	switch {
	case s.readGapOverlaps(sig, shard, start, end):
		reason = "missing its unflushed head"
	case s.wantOverlaps(sig, shard, start, end):
		reason = "missing a part it has not repaired"
	default:
		return nil
	}

	s.obs.RPC.ShardIncomplete(ctx, op)
	s.obs.Logger(ctx).Warn("held shard is incomplete, failing over",
		zap.String("op", op), zap.String("reason", reason),
		zap.Stringer("signal", sig), zap.String("shard", string(shard)))

	return cluster.ErrShardIncomplete
}

// closeCoveredGaps drops the gaps the shard's parts now cover: a flush drains the whole head, so
// parts reaching past a gap's bound carry everything that was missing from it. Run once per
// maintenance cycle, after the pass that loads the owner's freshly flushed parts.
func (s *Storage) closeCoveredGaps(ctx context.Context) {
	s.gapMu.RLock()
	open := make(map[gapKey]readGap, len(s.gaps))
	maps.Copy(open, s.gaps)
	s.gapMu.RUnlock()

	for k, g := range open {
		var partsMax int64
		if k.sig == signal.Metric {
			eng, ok := s.lookupEngine(k.shard)
			if !ok {
				continue
			}

			partsMax = metricPartsMax(eng)
		} else {
			eng, ok := s.lookupRecordEngine(k.sig, k.shard)
			if !ok {
				continue
			}

			partsMax = recordPartsMax(eng)
		}

		if partsMax <= g.after {
			continue
		}

		s.gapMu.Lock()
		delete(s.gaps, k)
		s.gapMu.Unlock()

		s.obs.Logger(ctx).Info("shard is complete again: parts now cover the head lost at recovery",
			zap.Stringer("signal", k.sig), zap.String("shard", string(k.shard)))
	}
}

// gapFetcher serves a shard from the local engine unless the request window falls in something the
// engine cannot answer for — a recovery gap or an unsatisfied want — in which case it fans out to
// the shard's other owners, the same failover a shard this node does not hold at all takes. Built
// only while the shard is incomplete.
//
// Its own disclaim is carried into the fan-out rather than discarded, because it is the fact that
// decides how the fan-out ends: this node holds the shard and knows it is short, so a fan-out where
// every peer also disclaims must fail the read rather than answer empty. With no peer to ask the
// read fails here, which is the single-node case of the same policy.
type gapFetcher struct {
	store   *Storage
	sig     signal.Signal
	shard   signal.TenantID
	local   fetch.Fetcher
	remotes []fetch.Fetcher
}

func (g gapFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	disclaim := g.store.canAnswer(ctx, rpcOpRead, g.sig, g.shard, true, r.Start, r.End)
	if disclaim == nil {
		return g.local.Fetch(ctx, r)
	}

	if len(g.remotes) == 0 {
		g.store.obs.RPC.ReadIncomplete(ctx, rpcOpRead)

		return nil, disclaim
	}

	return fetch.Filter(hedgedFetcher{
		store: g.store, op: rpcOpRead, remotes: g.remotes, selfDisclaim: disclaim,
	}).Fetch(ctx, r)
}

// gapGuarded wraps a shard's local read seam in its [gapFetcher] when the engine cannot answer for
// part of its range. In the steady state — nothing missing — it returns the engine itself, so the
// fetcher capabilities (series enumeration, label listing) stay intact.
//
// Unlike the placement checks it does not require a remote to fail over to: with no peer the read
// fails, which is the point. Serving the local engine there would answer a query short with no error.
func (s *Storage) gapGuarded(
	sig signal.Signal, shard signal.TenantID, local fetch.Fetcher, remotes []fetch.Fetcher,
) fetch.Fetcher {
	if !s.hasReadGap(sig, shard) && !s.hasWants(sig, shard) {
		return local
	}

	return gapFetcher{store: s, sig: sig, shard: shard, local: local, remotes: remotes}
}

// metricPartsMax returns the newest timestamp the engine's parts hold, or [math.MinInt64] when it
// has none.
func metricPartsMax(eng *engine.Engine) int64 {
	maxT := int64(math.MinInt64)
	for _, p := range eng.Parts() {
		maxT = max(maxT, p.MaxTime)
	}

	return maxT
}

// recordPartsMax is [metricPartsMax] for a record signal's engine.
func recordPartsMax(eng *recordengine.Engine) int64 {
	maxT := int64(math.MinInt64)
	for _, p := range eng.Parts() {
		maxT = max(maxT, p.MaxTime)
	}

	return maxT
}

// attachReadGap fills the shard's open read gap into its operator-facing stats, so a node failing
// over its own shards explains itself in Inspect rather than only in the logs.
func (s *Storage) attachReadGap(st *SignalStats, sig signal.Signal, shard signal.TenantID) {
	s.gapMu.RLock()
	defer s.gapMu.RUnlock()

	if g, ok := s.gaps[gapKey{sig: sig, shard: shard}]; ok {
		st.ReadGapAfterUnixNano, st.HasReadGap = g.after, true
	}
}

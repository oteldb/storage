package storage

import (
	"context"
	"maps"
	"math"

	"go.uber.org/zap"

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

// canAnswer reports whether this node may serve the shard for [start, end], metering and logging the
// failover when it may not. The enumeration RPCs spell "no time filter" as a zero window; widen it,
// since an unbounded listing certainly overlaps.
func (s *Storage) canAnswer(ctx context.Context, op string, sig signal.Signal, shard signal.TenantID, start, end int64) bool {
	if start == 0 && end == 0 {
		start, end = math.MinInt64, math.MaxInt64
	}

	if !s.readGapOverlaps(sig, shard, start, end) {
		return true
	}

	s.obs.RPC.ShardIncomplete(ctx, op)
	s.obs.Logger(ctx).Warn("held shard is missing its unflushed head, failing over",
		zap.String("op", op), zap.Stringer("signal", sig), zap.String("shard", string(shard)))

	return false
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

// gapFetcher serves a shard from the local engine unless the request window falls in the engine's
// recovery gap, in which case it fans out to the shard's other owners — the same failover a shard
// this node does not hold at all takes. Built only while a gap is open.
type gapFetcher struct {
	store  *Storage
	sig    signal.Signal
	shard  signal.TenantID
	local  fetch.Fetcher
	remote fetch.Fetcher
}

func (g gapFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if !g.store.canAnswer(ctx, rpcOpRead, g.sig, g.shard, r.Start, r.End) {
		return g.remote.Fetch(ctx, r)
	}

	return g.local.Fetch(ctx, r)
}

// gapGuarded wraps a shard's local read seam in its [gapFetcher] when the engine has an open
// recovery gap and there is another owner to ask. With no gap — the steady state — it returns the
// engine itself, so the fetcher capabilities (series enumeration, label listing) stay intact.
func (s *Storage) gapGuarded(
	sig signal.Signal, shard signal.TenantID, local fetch.Fetcher, remotes []fetch.Fetcher,
) fetch.Fetcher {
	if len(remotes) == 0 || !s.hasReadGap(sig, shard) {
		return local
	}

	return gapFetcher{
		store: s, sig: sig, shard: shard, local: local,
		remote: &filteringFetcher{inner: hedgedFetcher{store: s, op: rpcOpRead, remotes: remotes}},
	}
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

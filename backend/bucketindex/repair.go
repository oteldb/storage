package bucketindex

import "context"

// PartFetcher makes the parts discharging a repair cycle's wants local, so an engine can open them
// and commit them back into its index. It is the engine's whole view of the cluster during repair.
//
// A whole cycle goes in one call because the cluster-side cost is per cycle: one read of each
// peer's index answers every want, and one copy of a part discharges every want it contains.
//
// Any error is transient: an unreachable peer says nothing about whether the data exists. Only
// [WantAbsent] with a nil error can lead to a hole. The entry need not name the wanted prefix: a
// peer that merged the part away answers with the successor containing it, which is what makes
// repair terminate.
type PartFetcher interface {
	// FetchWants answers every want, returning one result per want in the same order.
	FetchWants(ctx context.Context, wants []Want) []FetchResult
}

// FetchResult is one want's outcome from a [PartFetcher].
type FetchResult struct {
	// Entry names the part that came across, valid only when Outcome is [WantSatisfied].
	Entry Entry
	// Outcome qualifies a fetch that brought nothing back.
	Outcome WantOutcome
	// Err is a transient failure; the want is retried on the next merge.
	Err error
}

// RepairStats counts what an engine's repair has done over its lifetime. Unsatisfiable climbing
// while Lost does not is the signal that repair is stuck rather than idle.
type RepairStats struct {
	// Local is the wants discharged with no network call, because the engine's own index gained a
	// part containing them.
	Local int64
	// Fetched is the parts pulled from a peer to discharge a want.
	Fetched int64
	// Unsatisfiable is the attempts that ended with no peer holding the part or any successor of
	// it: definitive absence, and an unrepaired shard.
	Unsatisfiable int64
	// Incomplete is the attempts that found nothing, but asked only a strict subset of the shard's
	// expected owners, so no evidence of loss accrues.
	Incomplete int64
	// Failed is the attempts that ended in a transient failure; the want is retried on the next
	// merge.
	Failed int64
	// Lost is the wants converted into a hole because no owner could supply the part: this node's
	// view of [Index.LostParts].
	Lost int64
	// Revoked is the holes replaced by the real part turning up after all.
	Revoked int64
}

// Add accumulates o into s.
func (s *RepairStats) Add(o RepairStats) {
	s.Local += o.Local
	s.Fetched += o.Fetched
	s.Unsatisfiable += o.Unsatisfiable
	s.Incomplete += o.Incomplete
	s.Failed += o.Failed
	s.Lost += o.Lost
	s.Revoked += o.Revoked
}

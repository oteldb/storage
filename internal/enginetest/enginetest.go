// Package enginetest is the conformance suite the metric and record engines share: the part
// lifecycle, index-commit and repair invariants both must hold, written once against an [Engine]
// adapter. The adapters live in each engine's external test package, so this package imports
// neither engine.
package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// Row is one sample or record. Val is the metric value or, for records, the body.
type Row struct {
	Stream string
	Ts     int64
	Val    int64
	// Attr is an optional key and value: a series label for metrics, a record attribute for records.
	Attr [2]string
}

// Config is what the suite varies when opening an engine; everything else is the adapter's.
type Config struct {
	Backend backend.Backend
	Repair  PartFetcher
	WAL     *wal.SegmentWriter
	Obs     *obs.Obs
}

// Part is one live part as [Engine.Parts] reports it.
type Part struct {
	ID               string
	MinTime, MaxTime int64
}

// Stats is the subset of the engine's stats the suite asserts on.
type Stats struct {
	HeadAge     time.Duration
	WantedParts int
	Holes       int
	LostParts   uint64
}

// MergeShape is the subset of the engine's merge shape the suite asserts on.
type MergeShape struct {
	Parts int
	Bytes int64
}

// RepairStats mirrors the engines' identical RepairStats, so an adapter converts rather than copies.
type RepairStats struct {
	Local, Fetched, Unsatisfiable, Incomplete, Failed, Lost, Revoked int64
}

// Engine is one engine adapted for the suite. Methods that share a name with the engine's own mean
// the same thing.
type Engine interface {
	Store
	Loader
	Introspector
	Repairer
}

// Store is the write, read and maintenance path.
type Store interface {
	// Append buffers rows in the head, failing t on error.
	Append(t *testing.T, rows ...Row)
	// Read returns every row stream holds, in the engine's fetch order, with Attr unset.
	Read(ctx context.Context, stream string) ([]Row, error)
	Flush(ctx context.Context) error
	Merge(ctx context.Context, retainFrom int64) error
	Reset(ctx context.Context) error
	HeadBytes() int64
}

// Loader is how an engine recovers state from its backend and WAL.
type Loader interface {
	LoadParts(ctx context.Context) error
	LoadPartsUnclaimed(ctx context.Context) error
	LoadPartsReadOnly(ctx context.Context) error
	RefreshReplica(ctx context.Context) error
	Replay(ctx context.Context, dir string) error
}

// Introspector is what the engine reports about itself.
type Introspector interface {
	// AttrNames returns every Attr key the engine can serve.
	AttrNames(t *testing.T) []string
	// StreamCount is the number of distinct stream identities the engine knows.
	StreamCount() int
	// HeadRows is the number of rows buffered in the head.
	HeadRows() int
	Parts() []Part
	PartCount() int
	PartPrefixes() []string
	Stats() Stats
	MergeShape() MergeShape
}

// Repairer is the repair-obligation surface, including the test-only LosePart and SetPartBlocks.
type Repairer interface {
	LosePart(prefix string, blocks bucketindex.Interval)
	AdoptWants(ws []bucketindex.Want)
	SetPartBlocks(prefix string, blocks bucketindex.Interval, level uint32)
	WantPrefixes() []string
	HasWants() bool
	WantOverlaps(start, end int64) bool
	Holes() []bucketindex.Entry
	LostParts() uint64
	RepairStats() RepairStats
}

// Kind is one engine the suite runs against.
type Kind struct {
	// Name names the subtest: "metrics" or "records".
	Name string
	// Prefix is the backend key prefix every engine Open returns writes under.
	Prefix string
	Open   func(t *testing.T, cfg Config) Engine

	// Identity is the identity the engine gives stream.
	Identity func(stream string) signal.Series
	// LegacyIdentityObject is the whole-set identity object older builds wrote at the prefix.
	LegacyIdentityObject string
	// Erodes reports whether an object key is one a partially lost part no longer has.
	Erodes func(key string) bool
	// IdentitySeed and IdentityRatio tune [Run]'s write-amplification check: the second flush of one
	// new stream must write under 1/IdentityRatio of the first flush's IdentitySeed streams.
	IdentitySeed, IdentityRatio int
}

func (k Kind) indexKey() string { return k.Prefix + "/" + bucketindex.Object }

func (k Kind) open(t *testing.T, be backend.Backend) Engine {
	t.Helper()

	return k.Open(t, Config{Backend: be})
}

var suite = []struct {
	name string
	run  func(t *testing.T, k Kind)
}{
	{"GonePartBecomesWant", gonePartBecomesWant},
	{"PartiallyGonePartBecomesWant", partiallyGonePartBecomesWant},
	{"TransientOpenFailureRecordsNoWant", transientOpenFailureRecordsNoWant},
	{"CorruptPartIsNotAWant", corruptPartIsNotAWant},
	{"WantIsRecordedInOneCommit", wantIsRecordedInOneCommit},
	{"FailedWantCommitAppliesNeither", failedWantCommitAppliesNeither},
	{"WantSurvivesLaterCommits", wantSurvivesLaterCommits},
	{"EntriesLeaveOnlyIntoRemovedOrWanted", entriesLeaveOnlyIntoRemovedOrWanted},

	{"FailedFlushBurnsPartID", failedFlushBurnsPartID},
	{"LoadPartsSweepsOrphanParts", loadPartsSweepsOrphanParts},
	{"LoadPartsKeepsLiveParts", loadPartsKeepsLiveParts},
	{"RefreshReplicaKeepsUncommittedParts", refreshReplicaKeepsUncommittedParts},
	{"MergeIndexCommitFailureKeepsSources", mergeIndexCommitFailureKeepsSources},
	{"PartsSyncedBeforeIndexCommit", partsSyncedBeforeIndexCommit},
	{"FlushFailureKeepsRows", flushFailureKeepsRows},
	{"FlushFailureKeepsRowsAcrossRestart", flushFailureKeepsRowsAcrossRestart},
	{"FlushFailureMergesConcurrentAppends", flushFailureMergesConcurrentAppends},
	{"PublishCommitsBucketIndexLast", publishCommitsBucketIndexLast},
	{"UncommittedPartIdentityIsNotLoaded", uncommittedPartIdentityIsNotLoaded},
	{"PublishWritesIdentityBeforeCommit", publishWritesIdentityBeforeCommit},

	{"ResetWaitsForInFlightFlush", resetWaitsForInFlightFlush},
	{"ResetKeepsPartsUnderRead", resetKeepsPartsUnderRead},
	{"ConcurrentFlushIsSerialized", concurrentFlushIsSerialized},
	{"PartIdentityRetentionSelfCleaning", partIdentityRetentionSelfCleaning},
	{"PartIdentityMergedPartCarriesUnion", partIdentityMergedPartCarriesUnion},
	{"LegacyIdentityObjectStillResolves", legacyIdentityObjectStillResolves},
	{"LegacyIdentityObjectDeletedOnceMigrated", legacyIdentityObjectDeletedOnceMigrated},
	{"PartIdentityWriteAmplification", partIdentityWriteAmplification},
	{"WALResolvesStreamAfterCheckpoint", walResolvesStreamAfterCheckpoint},
	{"HeadAgeTracksFlushLag", headAgeTracksFlushLag},
	{"MergeShapeReportsBytes", mergeShapeReportsBytes},

	{"HoleCommittedAfterRepeatedAbsence", holeCommittedAfterRepeatedAbsence},
	{"IncompletePeerSetNeverHoles", incompletePeerSetNeverHoles},
	{"TransientFailureNeverHoles", transientFailureNeverHoles},
	{"AbsenceEvidenceResetsOnAnyOtherOutcome", absenceEvidenceResetsOnAnyOtherOutcome},
	{"HoleRevokedByExactPrefix", holeRevokedByExactPrefix},
	{"HoleRevokedByLostSuccessor", holeRevokedByLostSuccessor},
	{"HoleRevokedByContainingSuccessor", holeRevokedByContainingSuccessor},
	{"HoleSurvivesReload", holeSurvivesReload},
	{"HoleNotOfferedToAPeer", holeNotOfferedToAPeer},
	{"RepairStatsSurfaceLoss", repairStatsSurfaceLoss},
	{"WantOverlapIsBoundedByTheLostPart", wantOverlapIsBoundedByTheLostPart},
	{"CommittedHoleLetsReadsThrough", committedHoleLetsReadsThrough},
	{"TransientErrorBreaksAbsenceRun", transientErrorBreaksAbsenceRun},
	{"UnattemptedWantKeepsAbsenceEvidence", unattemptedWantKeepsAbsenceEvidence},
	{"RepairConcurrentMergesFetchOnce", repairConcurrentMergesFetchOnce},
	{"WantsPastBoundStayOwed", wantsPastBoundStayOwed},
	{"RefreshReplicaGonePartBecomesPendingWant", refreshReplicaGonePartBecomesPendingWant},
	{"BlockNumbersSurviveAnEmptiedShard", blockNumbersSurviveAnEmptiedShard},
	{"RepairFetchesWantedPartFromPeer", repairFetchesWantedPartFromPeer},
	{"RepairDischargedByContainingSuccessor", repairDischargedByContainingSuccessor},
	{"RepairDischargedByLocalPart", repairDischargedByLocalPart},
	{"RepairNoPeerLeavesWant", repairNoPeerLeavesWant},
	{"RepairTransientFailureKeepsWant", repairTransientFailureKeepsWant},
	{"RepairWithoutCallbackIsNoOp", repairWithoutCallbackIsNoOp},
	{"RepairUnreadablePartKeepsWant", repairUnreadablePartKeepsWant},
	{"RepairCommitFailureKeepsWant", repairCommitFailureKeepsWant},
	{"RepairAsksTheFetcherOncePerCycle", repairAsksTheFetcherOncePerCycle},
	{"RepairCoveredWantIsNotAFailure", repairCoveredWantIsNotAFailure},
	{"RepairFailedCommitObservesNoLoss", repairFailedCommitObservesNoLoss},
	{"RepairUnopenablePartObservedAsFailed", repairUnopenablePartObservedAsFailed(phantomCopy)},
	{"RepairTruncatedPartObservedAsFailed", repairUnopenablePartObservedAsFailed(truncatedCopyOf)},
	{"RepairDischargedByPartHeldOnDisk", repairDischargedByPartHeldOnDisk},
	{"LoadPartsUnclaimedDefersTheWant", loadPartsUnclaimedDefersTheWant},
	{"LoadPartsUnclaimedWantIsRepaired", loadPartsUnclaimedWantIsRepaired},
	{"LoadPartsReadOnlySweepsNothing", loadPartsReadOnlySweepsNothing},
	{"AdoptedWantIsRepairedIntoTheIndex", adoptedWantIsRepairedIntoTheIndex},
	{"AdoptWantsIgnoresWhatIsAlreadyHere", adoptWantsIgnoresWhatIsAlreadyHere},
	{"RefreshReplicaTrimsPerStream", refreshReplicaTrimsPerStream},
	{"PromotedReplicaKeepsLateRow", promotedReplicaKeepsLateRow},
	{"MidFlushAppendSurvivesCrash", midFlushAppendSurvivesCrash},
}

// Run runs every suite test against k, each as <test>/<k.Name>.
func Run(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range suite {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Run(k.Name, func(t *testing.T) {
				t.Parallel()
				tc.run(t, k)
			})
		})
	}
}

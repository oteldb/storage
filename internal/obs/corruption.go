package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Dispositions of a corrupt artifact, the disposition attribute of storage.corruption.detected.
const (
	// CorruptTolerated is corruption the engine fell back from and served on.
	CorruptTolerated = "tolerated"
	// CorruptFatal is corruption that failed the operation that met it.
	CorruptFatal = "fatal"
	// CorruptWanted is a part that stayed corrupt across consecutive loads and was handed to repair
	// as a want, as a part whose objects are gone is.
	CorruptWanted = "wanted"
)

// Reasons a load fenced an engine, the reason attribute of storage.index.fenced_loads.
const (
	// FenceCorrupt is a corrupt bucket index or part.
	FenceCorrupt = "corrupt"
	// FenceUnsupportedVersion is a part written in a format newer than this binary reads.
	FenceUnsupportedVersion = "unsupported_version"
	// FenceUnavailable is a backend that failed to answer.
	FenceUnavailable = "unavailable"
)

// Reasons a tenure's establishing commit failed, the reason attribute of
// storage.index.establish_failures.
const (
	// EstablishConflict is a commit that lost the index CAS to another writer.
	EstablishConflict = "conflict"
	// EstablishUnavailable is a load or commit the backend failed to answer.
	EstablishUnavailable = "unavailable"
)

// Corruption instruments on-disk artifacts that failed their integrity checks, and the index
// commits an engine is refusing because of them or of its tenure. It is counted where the failure
// is handled rather than where a codec detects it: only the consuming layer knows whether the engine
// fell back or failed.
type Corruption struct {
	detected          metric.Int64Counter
	fencedLoads       metric.Int64Counter
	establishFailures metric.Int64Counter
}

// Detected accounts one corrupt artifact: component names what failed its check (wal, part,
// bucket_index, marks, bloom, part_identity, series_index, series_stats, stream_order), disposition is
// [CorruptTolerated], [CorruptFatal] or [CorruptWanted].
func (c *Corruption) Detected(ctx context.Context, component, disposition string) {
	c.detected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("component", component), attribute.String("disposition", disposition)))
}

// FencedLoad accounts one index load that failed and left an engine of sig refusing to commit, for
// reason ([FenceCorrupt], [FenceUnsupportedVersion] or [FenceUnavailable]). A fenced engine retries
// every maintenance cycle, so a steady rate is one engine that stays fenced.
func (c *Corruption) FencedLoad(ctx context.Context, sig, reason string) {
	c.fencedLoads.Add(ctx, 1, metric.WithAttributes(
		attribute.String("signal", sig), attribute.String("reason", reason)))
}

// EstablishFailed accounts one attempt of an engine of sig to make its tenure's first commit that
// did not land, for reason ([EstablishConflict] or [EstablishUnavailable]). Until one lands the
// engine commits nothing, and it tries once per flush and once per merge.
func (c *Corruption) EstablishFailed(ctx context.Context, sig, reason string) {
	c.establishFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("signal", sig), attribute.String("reason", reason)))
}

func newCorruption(m metric.Meter) (*Corruption, error) {
	b := &imb{m: m}
	c := &Corruption{
		detected: b.counter("storage.corruption.detected",
			"on-disk artifacts that failed an integrity check, by component and disposition", "{artifact}"),
		fencedLoads: b.counter("storage.index.fenced_loads",
			"index loads that failed and left the engine refusing commits until a load succeeds", "{load}"),
		establishFailures: b.counter("storage.index.establish_failures",
			"attempts to make a new tenure's first index commit that did not land", "{attempt}"),
	}

	return c, b.err
}

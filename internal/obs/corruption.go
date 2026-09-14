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
)

// Corruption instruments on-disk artifacts that failed their integrity checks. It is counted where
// the corruption is handled rather than where a codec detects it: only the consuming layer knows
// whether the engine fell back or failed.
type Corruption struct {
	detected metric.Int64Counter
}

// Detected accounts one corrupt artifact: component names what failed its check (wal, part,
// bucket_index, marks, bloom, part_identity, series_index, series_stats, stream_order), disposition is
// [CorruptTolerated] or [CorruptFatal].
func (c *Corruption) Detected(ctx context.Context, component, disposition string) {
	c.detected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("component", component), attribute.String("disposition", disposition)))
}

func newCorruption(m metric.Meter) (*Corruption, error) {
	b := &imb{m: m}
	c := &Corruption{
		detected: b.counter("storage.corruption.detected",
			"on-disk artifacts that failed an integrity check, by component and disposition", "{artifact}"),
	}

	return c, b.err
}

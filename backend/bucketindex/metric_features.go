package bucketindex

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

// MetricFeature fences readers that predate a metric format.
type MetricFeature uint64

const (
	// MetricFeaturePackedWAL allows packed references in the standalone metric WAL.
	MetricFeaturePackedWAL MetricFeature = 1 << iota
	metricFeatureCompactMetadata

	knownMetricFeatures = MetricFeaturePackedWAL
)

func appendMetricFeatures(dst []byte, ix *Index) []byte {
	flags := ix.MetricFeatures
	if ix.CompactMetricMetadata {
		flags |= metricFeatureCompactMetadata
	}
	return binary.AppendUvarint(dst, uint64(flags))
}

func decodeMetricFeatures(ix *Index, buf []byte) ([]byte, error) {
	flags, rest, ok := readUvarint(buf)
	if !ok || MetricFeature(flags)&knownMetricFeatures == 0 {
		return nil, errors.Wrap(ErrCorrupt, "invalid metric feature flags")
	}
	ix.CompactMetricMetadata = MetricFeature(flags)&metricFeatureCompactMetadata != 0
	ix.MetricFeatures = MetricFeature(flags) &^ metricFeatureCompactMetadata
	return rest, nil
}

func validateMetricFeatures(ix *Index) error {
	if ix.MetricFeatures&^knownMetricFeatures != 0 {
		return errors.Wrap(ErrCorrupt, "unsupported metric feature flags")
	}
	if ix.MetricFeatures&MetricFeaturePackedWAL != 0 && !ix.CompactMetricMetadata {
		return errors.Wrap(ErrCorrupt, "packed WAL requires compact metric metadata")
	}
	return nil
}

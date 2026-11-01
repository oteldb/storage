package engine

import (
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
)

type metricMergeCoders struct {
	frames, objects *compress.Compressor
}

func newMetricMergeCoders(src []*part, profile compressProfile) metricMergeCoders {
	var identities int64
	for _, p := range src {
		identities = buildbudget.Add(identities, int64(p.index.seriesCount()))
	}
	// The only whole compressed column is the series ID run list, at most one run per input ID.
	objectBytes := buildbudget.Add(32, buildbudget.Mul(identities, 32))
	return metricMergeCoders{
		frames:  block.NewFrameCompressor(profile.Algorithm, profile.Level),
		objects: compress.NewFrameCompressor(profile.Algorithm, profile.Level, int(min(objectBytes, int64(maxInt)))),
	}
}

func (c metricMergeCoders) workspace() int64 {
	return buildbudget.Add(c.frames.EncodeWorkspace(), c.objects.EncodeWorkspace())
}

func (c metricMergeCoders) options() []block.PartOption {
	return []block.PartOption{block.WithCompressors(c.objects), block.WithFrameCompressors(c.frames)}
}

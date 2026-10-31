package recordengine

import (
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/retention"
	"github.com/oteldb/storage/internal/timebucket"
)

func (e *Engine) selectMergeParts(src []*part, opts MergeOptions, capBytes int64) []*part {
	return e.selectMergePartsProfile(src, opts, capBytes, e.mergeProfile())
}

func (e *Engine) selectMergePartsProfile(src []*part, opts MergeOptions, capBytes int64, profile mergeProfile) []*part {
	opts = e.retentionOptions(opts)
	idle := int(e.idleMerges.Load())
	if opts.Force {
		idle = mergeIdleRounds
	}
	due := func(p *part) bool {
		return retentionForces(p, opts.RetainFrom) && retention.Due(p.retentionAt, opts.retentionNow)
	}
	sealedForRetention := func(p *part) bool {
		return sealed(p, capBytes) || (!opts.retentionNow.IsZero() && !due(p))
	}
	if selected := timebucket.Forced(src, partSpan, (*part).sizeBytes, due, sealedForRetention,
		capBytes, maxTierParts); len(selected) > 0 {
		return selected
	}
	var expected func([]*part) int
	if !e.mergeFeedback.empty() {
		expected = func(run []*part) int { return e.mergeFeedback.expected(run, profile, capBytes) }
	}
	if selected := selectLadderGroupExpected(src, capBytes, idle, expected); len(selected) > 0 {
		return selected
	}
	if selected := selectStraddlers(src, capBytes); len(selected) > 0 {
		return selected
	}
	return timebucket.Forced(src, partSpan, (*part).sizeBytes, e.compressionForces,
		// Balanced compaction already had its turn; do not fill a compression-only rewrite with cold neighbors.
		func(*part) bool { return true }, capBytes, maxTierParts)
}

func (e *Engine) compressionForces(p *part) bool {
	algorithm, level := e.cfg.MergeCompression, e.cfg.MergeCompressionLevel
	if algorithm == compress.AlgorithmNone {
		return false
	}

	columns := p.reader.Manifest().Columns
	for i := range columns {
		c := &columns[i]
		if c.Const {
			continue
		}
		if c.Compress != algorithm || (algorithm == compress.AlgorithmZSTD && zstdLevel(c.Level) < zstdLevel(level)) {
			return true
		}
	}

	return false
}

func zstdLevel(level compress.Level) compress.Level {
	if level == compress.LevelDefault {
		return 3
	}
	return level
}

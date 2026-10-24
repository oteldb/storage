package recordengine

import (
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/timebucket"
)

func (e *Engine) selectMergeParts(src []*part, retainFrom, capBytes int64, force bool) []*part {
	idle := int(e.idleMerges.Load())
	if force {
		idle = mergeIdleRounds
	}
	if selected := selectMergePartsIdle(src, retainFrom, capBytes, idle); len(selected) > 0 {
		return selected
	}

	return timebucket.Forced(src, partSpan, (*part).sizeBytes, e.compressionForces,
		func(p *part) bool { return sealed(p, capBytes) }, capBytes, maxTierParts)
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

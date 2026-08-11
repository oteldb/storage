package engine

import "github.com/oteldb/storage/encoding/compress"

// RecompressSpec is the absolute (wall-clock-free) form of a tenant recompression policy: a merged
// part whose newest sample is older than Before (it is fully cold) is written with Algorithm at
// Level instead of the ladder level its size would select. The level is decode-irrelevant — the
// reader reconstructs the decompressor from the per-column algorithm recorded in the manifest — so
// this is a pure ratio/CPU trade-off with no format change. The caller ([storage.Storage]) builds it
// from [tenant.Recompress] and the current time.
type RecompressSpec struct {
	Before    int64 // a part whose maxTime < Before is cold and is rewritten with the profile below
	Algorithm compress.Algorithm
	Level     compress.Level
}

// compressProfile is the block-compression setting a part is written with. The zero value is
// codec-only framing (no block compression), which is what a hot flush uses.
type compressProfile struct {
	Algorithm compress.Algorithm
	Level     compress.Level
}

// Compression-ladder thresholds in rows. A merge rewrites the data anyway, so compressing it costs
// only the level's own CPU, and a bigger part earns a denser level: it holds older data, is read
// less often per byte, and is merged again less often. VictoriaMetrics does the same by rows per
// block and caps at zstd 3 (lib/storage/partition.go getCompressLevel); levels above that buy
// single-digit percent for roughly an order of magnitude more CPU, which on a small node competes
// with merge and retention. An age tier above 3 exists, but only through [RecompressSpec].
const (
	ladderRowsFast   = 1 << 16 // ≤ 64k rows: a small, still-hot merge output
	ladderRowsMedium = 1 << 20 // ≤ 1M rows
)

// ladderLevel returns the compression level a merged part of the given row count is written at.
func ladderLevel(rows int) compress.Level {
	switch {
	case rows <= ladderRowsFast:
		return 1
	case rows <= ladderRowsMedium:
		return 2
	default:
		return 3
	}
}

// mergeProfile returns the compression profile a merged part of rows rows, whose newest sample is
// maxTime, is written with: the size-graduated ladder, or the recompression profile when the part is
// fully cold (its newest sample predates the cutoff) and that profile is denser.
func mergeProfile(spec *RecompressSpec, maxTime int64, rows int) compressProfile {
	p := compressProfile{Algorithm: compress.AlgorithmZSTD, Level: ladderLevel(rows)}

	if spec == nil || maxTime >= spec.Before {
		return p
	}

	if spec.Algorithm != p.Algorithm || spec.Level > p.Level {
		return compressProfile{Algorithm: spec.Algorithm, Level: spec.Level}
	}

	return p
}

// recompressApplies reports whether a (single) part should be rewritten to apply recompression: it
// is fully cold and is not already at the target algorithm and level. That comparison is the fixed
// point that stops a lone cold part from being rewritten on every merge tick — it is why the level
// is recorded in the manifest at all. Only an *upgrade* forces a rewrite: a part already denser than
// the target (an older, more aggressive policy) is left alone rather than rewritten to a cheaper
// level for nothing.
func recompressApplies(p *part, spec *RecompressSpec) bool {
	if spec == nil || p.maxTime >= spec.Before {
		return false
	}

	alg, level := p.compressedAt()

	return alg != spec.Algorithm || level < spec.Level
}

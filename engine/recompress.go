package engine

import "github.com/oteldb/storage/encoding/compress"

// RecompressSpec is the absolute (wall-clock-free) form of a tenant recompression policy: a merged
// part whose newest sample is older than Before (it is fully cold) is written with Algorithm at
// Level instead of the default codec-only framing. The level is decode-irrelevant — the reader
// reconstructs the decompressor from the per-column algorithm recorded in the manifest — so this is
// a pure ratio/CPU trade-off with no format change. The caller ([storage.Storage]) builds it from
// [tenant.Recompress] and the current time.
type RecompressSpec struct {
	Before    int64 // a part whose maxTime < Before is cold and is rewritten with the profile below
	Algorithm compress.Algorithm
	Level     compress.Level
}

// recompressApplies reports whether a (single) part should be rewritten to apply recompression: it
// is fully cold and not already compressed with the target algorithm. The second test is the fixed
// point that stops a lone cold part from being rewritten on every merge tick.
func recompressApplies(p *part, spec *RecompressSpec) bool {
	if spec == nil || p.maxTime >= spec.Before {
		return false
	}

	return p.compressedWith() != spec.Algorithm
}

// coldProfile returns the compression profile to write a freshly-merged part with: the recompress
// profile when the part is fully cold (its newest sample predates the cutoff), else nil (the
// default codec-only framing for parts that still hold warm data).
func coldProfile(spec *RecompressSpec, maxTime int64) *RecompressSpec {
	if spec == nil || maxTime >= spec.Before {
		return nil
	}

	return spec
}

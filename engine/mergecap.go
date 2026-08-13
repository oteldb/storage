package engine

import (
	"context"
	"math"

	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memlimit"
)

// The merge cap is the size a merged part grows to before it is sealed, in bytes on disk. It is
// bounded by both the free space and the memory a merge may hold; ARCH.md ("Flush and merge are
// bounded separately") has the reasoning and the references.
const (
	// defaultMergeCeilingBytes is a safety net rather than the usual control — free space or memory
	// normally binds first — keeping a part on a very large disk from growing slow to merge and
	// coarse to prune.
	defaultMergeCeilingBytes = 16 << 30

	// freeSpaceDivisor leaves room for a merge's output to coexist with the inputs it has not yet
	// retired. ClickHouse reserves against the same hazard with DISK_USAGE_COEFFICIENT_TO_SELECT.
	freeSpaceDivisor = 2

	// mergeMemoryFraction is the reciprocal share of the process memory limit that all concurrent
	// merges together may hold as output buffers. The rest of the limit belongs to the head, the
	// caches and the decode budget, so the merge takes a minority slice.
	mergeMemoryFraction = 8

	// mergeBufferAmplification converts a merge's memory allowance into a seal threshold. A merged
	// part is buffered *encoded* in RAM until it is sealed and then serialized into one buffer per
	// column, so the peak is about twice the sealed size (plus the rival-codec accumulator on the
	// value column, which the rounding here absorbs).
	mergeBufferAmplification = 2

	// defaultMergeMemoryBytes is the merge allowance on a platform where no memory budget can be
	// detected at all — no GOMEMLIMIT, no cgroup, no host total (see [memlimit.Bytes]).
	defaultMergeMemoryBytes = 512 << 20
)

// minMergeCapBytes floors the derived cap, so a nearly full disk or a tight memory limit degrades to
// small merges rather than to no merging at all.
const minMergeCapBytes = 8 << 20

// mergeCapBytes returns the on-disk size at which a merged part is sealed: the configured ceiling,
// lowered to this merge's share of free space when the backend can report it, and to what this
// merge may hold in memory. A negative ceiling disables sealing entirely.
func (e *Engine) mergeCapBytes(ctx context.Context) int64 {
	ceiling := e.cfg.MergeCeilingBytes
	switch {
	case ceiling < 0:
		return 0 // unlimited: never seal
	case ceiling == 0:
		ceiling = defaultMergeCeilingBytes
	}

	concurrency := e.mergeConcurrency()

	derived := e.mergeMemoryCapBytes(concurrency)

	free, err := backend.FreeSpace(ctx, e.cfg.Backend)
	switch {
	case err != nil:
		if !errors.Is(err, backend.ErrSpaceUnknown) {
			zctx.From(ctx).Warn("free space unavailable; merge cap falls back to the ceiling",
				zap.String("prefix", e.cfg.Prefix), zap.Error(err))
		}
	default:
		// Nearly full keeps merging small groups rather than sealing everything: stranding the part
		// count high is worst exactly when compaction matters most.
		derived = min(derived, max(free/int64(concurrency)/freeSpaceDivisor, minMergeCapBytes))
	}

	// The floor applies to the derived bounds, not to the ceiling: an embedder that configures a cap
	// below it means it.
	return min(ceiling, max(derived, minMergeCapBytes))
}

// mergeMemoryCapBytes is the part size this merge can seal at without the output buffer outgrowing
// the memory the process has to spare. Free space says nothing about it: the merge writer holds the
// whole encoded output part in RAM, so on a node whose disk dwarfs its memory limit — a 4 GiB pod
// over a 464 GiB volume — the disk-derived share alone sizes a part the process cannot hold.
func (e *Engine) mergeMemoryCapBytes(concurrency int) int64 {
	budget := e.cfg.MergeMemoryBytes
	switch {
	case budget < 0:
		return math.MaxInt64 // explicitly unbounded: the caller accounts for merge memory elsewhere
	case budget == 0:
		budget = defaultMergeMemoryBytes
		if limit := memlimit.Bytes(); limit > 0 {
			budget = limit / mergeMemoryFraction
		}
	}

	return budget / int64(concurrency) / mergeBufferAmplification
}

// mergeConcurrency is how many merges may be running against this engine's backend at once, per
// [Config.MergeConcurrency]. It divides both the disk and the memory a single merge may claim.
func (e *Engine) mergeConcurrency() int {
	if e.cfg.MergeConcurrency == nil {
		return 1
	}

	return max(e.cfg.MergeConcurrency(), 1)
}

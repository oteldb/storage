package engine

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/backend"
)

// The merge cap is the size a merged part grows to before it is sealed, in bytes on disk. It is
// derived from free space rather than held at a constant; ARCH.md ("Flush and merge are bounded
// separately") has the reasoning and the references.
const (
	// defaultMergeCeilingBytes is a safety net rather than the usual control — free space normally
	// binds first — keeping a part on a very large disk from growing slow to merge and coarse to
	// prune.
	defaultMergeCeilingBytes = 16 << 30

	// freeSpaceDivisor leaves room for a merge's output to coexist with the inputs it has not yet
	// retired. ClickHouse reserves against the same hazard with DISK_USAGE_COEFFICIENT_TO_SELECT.
	freeSpaceDivisor = 2
)

// mergeCapBytes returns the on-disk size at which a merged part is sealed: the configured ceiling,
// lowered to this merge's share of free space when the backend can report it. A negative ceiling
// disables sealing entirely.
func (e *Engine) mergeCapBytes(ctx context.Context) int64 {
	ceiling := e.cfg.MergeCeilingBytes
	switch {
	case ceiling < 0:
		return 0 // unlimited: never seal
	case ceiling == 0:
		ceiling = defaultMergeCeilingBytes
	}

	free, err := backend.FreeSpace(ctx, e.cfg.Backend)
	if err != nil {
		if !errors.Is(err, backend.ErrSpaceUnknown) {
			zctx.From(ctx).Warn("free space unavailable; merge cap falls back to the ceiling",
				zap.String("prefix", e.cfg.Prefix), zap.Error(err))
		}

		return ceiling
	}

	concurrency := 1
	if e.cfg.MergeConcurrency != nil {
		concurrency = max(e.cfg.MergeConcurrency(), 1)
	}

	share := free / int64(concurrency) / freeSpaceDivisor
	if share <= 0 {
		// Nearly full: keep merging small groups. Sealing everything would strand the part count
		// high exactly when compaction matters most.
		return minMergeCapBytes
	}

	return min(ceiling, max(share, minMergeCapBytes))
}

// minMergeCapBytes floors the disk-derived cap, so a nearly full disk degrades to small merges
// rather than to no merging at all.
const minMergeCapBytes = 8 << 20

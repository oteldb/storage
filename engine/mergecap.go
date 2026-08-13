package engine

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/backend"
)

// The merge cap is the size a merged part grows to before it is sealed, in bytes on disk.
//
// Sizing it by a constant makes it correct at exactly one deployment size. Cardinality consumes a
// byte budget in *breadth* — every active series takes a slice of every part — so with a fixed cap
// the time span a part covers is inversely proportional to active series, and a fixed-range query
// touches proportionally more parts as cardinality grows. That is the property a TSDB most needs
// not to have.
//
// Both references size the merge against the storage instead: VictoriaMetrics takes free space
// divided by the merge worker count, capped by a constant (lib/storage/partition.go
// getMaxOutBytes); ClickHouse takes free space divided by a safety coefficient, capped by
// max_bytes_to_merge_at_max_space_in_pool. Dividing by the worker count is what keeps several
// concurrent merges from collectively filling the disk.
const (
	// defaultMergeCeilingBytes bounds a merged part when nothing smaller applies. It is a safety
	// net rather than the usual control — free space is normally what binds — but it keeps a part
	// from growing without limit on a very large disk, where a single part would become slow to
	// merge and coarse to prune.
	defaultMergeCeilingBytes = 16 << 30 // 16 GiB

	// freeSpaceDivisor is applied on top of MergeConcurrency, so a merge may consume at most half
	// of its share of the remaining space. A merge writes its output before retiring its inputs, so
	// it needs room for both at once; ClickHouse reserves against the same hazard with
	// DISK_USAGE_COEFFICIENT_TO_SELECT.
	freeSpaceDivisor = 2
)

// mergeCapBytes returns the on-disk size at which a merged part is sealed: the configured ceiling,
// lowered to the backend's share of free space when the backend can report it. A backend that
// cannot (memory, object stores — where local free space has no meaning) keeps the ceiling.
//
// A negative ceiling disables sealing entirely, which is how a caller asks for the old
// unlimited-part behavior.
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
		// Nearly full: keep merging, but only the smallest groups. Sealing everything instead would
		// strand the part count high exactly when compaction is most needed.
		return minMergeCapBytes
	}

	return min(ceiling, max(share, minMergeCapBytes))
}

// minMergeCapBytes is the floor the disk-derived cap is never taken below, so a nearly full disk
// degrades to small merges rather than to no merging at all.
const minMergeCapBytes = 8 << 20 // 8 MiB

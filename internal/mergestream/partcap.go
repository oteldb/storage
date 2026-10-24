package mergestream

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// DefaultPartCeiling is the maximum merged part size when no ceiling is configured.
const DefaultPartCeiling int64 = 16 << 30

// MinPartCap keeps low free space from excluding every existing part from compaction.
const MinPartCap int64 = 8 << 20

// PartCap derives a disk seal threshold from free space and the concurrent writer count.
// A negative ceiling disables disk sealing. An error still returns the configured ceiling.
func PartCap(ctx context.Context, be backend.Backend, ceiling int64, fanout int) (int64, error) {
	switch {
	case ceiling < 0:
		return 0, nil
	case ceiling == 0:
		ceiling = DefaultPartCeiling
	}
	free, err := backend.FreeSpace(ctx, be)
	if errors.Is(err, backend.ErrSpaceUnknown) {
		return ceiling, nil
	}
	if err != nil {
		return ceiling, err
	}
	return min(ceiling, max(free/int64(max(fanout, 1))/2, MinPartCap)), nil
}

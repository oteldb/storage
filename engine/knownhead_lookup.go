package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

var errKnownIdentityMissing = errors.New("engine: known head identity missing from pinned metadata")

// lookupKnownBatch opens one forward reader per root, sharing pages across sorted IDs.
func (s *seriesSnapshot) lookupKnownBatch(ctx context.Context, ids []signal.SeriesID, visit func(signal.SeriesID, []byte) error) error {
	pending := slices.Clone(ids)
	slices.SortFunc(pending, signal.SeriesID.Compare)
	pending = slices.Compact(pending)
	if s != nil {
		for _, root := range s.roots {
			if len(pending) == 0 {
				return nil
			}
			var err error
			pending, err = lookupKnownRoot(ctx, root, pending, visit)
			if err != nil {
				return err
			}
		}
	}
	if len(pending) != 0 {
		return errKnownIdentityMissing
	}
	return nil
}

func lookupKnownRoot(
	ctx context.Context, root *seriesRootHandle, ids []signal.SeriesID, visit func(signal.SeriesID, []byte) error,
) (remaining []signal.SeriesID, err error) {
	r, err := root.run.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	r.StreamPages()
	remaining = ids[:0]
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, found, err := r.LookupRaw(id)
		if err != nil {
			return nil, err
		}
		if !found {
			remaining = append(remaining, id)
			continue
		}
		if err := visit(id, raw); err != nil {
			return nil, err
		}
	}
	return remaining, nil
}

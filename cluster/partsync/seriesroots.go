package partsync

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

var errSeparateSeries = errors.New("partsync: separate series catalogs are unsupported")

func rejectSeriesCatalogBytes(raw []byte) error {
	if len(raw) >= 3 && raw[0] == 'B' && raw[1] == 'I' && raw[2] >= 9 {
		return errSeparateSeries
	}
	return nil
}

func rejectSeriesRoots(ix *bucketindex.Index) error {
	if len(ix.SeriesRoots) > 0 || len(ix.RetiredSeriesRoots) > 0 {
		return errSeparateSeries
	}
	return nil
}

func failedWants(out []WantResult, err error) []WantResult {
	for i := range out {
		out[i].Err = err
	}
	return out
}

func (s *Syncer) localMirrorIndex(ctx context.Context, key string) ([]byte, *bucketindex.Index, error) {
	raw, err := backend.ReadView(ctx, s.local, key)
	if err != nil && !errors.Is(err, backend.ErrNotExist) {
		return nil, nil, errors.Wrap(err, "read local index")
	}
	if err := rejectSeriesCatalogBytes(raw); err != nil {
		return nil, nil, err
	}
	ix := &bucketindex.Index{}
	if raw != nil {
		if decoded, err := bucketindex.Decode(raw); err == nil {
			ix = decoded
		} // A corrupt local index is treated as empty and overwritten by the mirror.
	}
	return raw, ix, rejectSeriesRoots(ix)
}

func (s *Syncer) repairIndexes(
	ctx context.Context, enginePrefix string, peers []string,
) ([]peerView, *bucketindex.Index, error) {
	views, firstErr := s.peerIndexes(ctx, enginePrefix, peers)
	if errors.Is(firstErr, errSeparateSeries) {
		return nil, nil, firstErr
	}
	// Failed local lineage cannot support definitive absence, but readable peers can still repair.
	local, err := s.localRepairIndex(ctx, enginePrefix+"/"+bucketindex.Object)
	if errors.Is(err, errSeparateSeries) {
		return nil, nil, err
	}
	if err != nil {
		local = &bucketindex.Index{}
		if firstErr == nil {
			firstErr = errors.Wrap(err, "load local index")
		}
	}
	if err := rejectSeriesRoots(local); err != nil {
		return nil, nil, err
	}
	return views, local, firstErr
}

func (s *Syncer) localRepairIndex(ctx context.Context, key string) (*bucketindex.Index, error) {
	raw, err := backend.ReadView(ctx, s.local, key)
	if errors.Is(err, backend.ErrNotExist) {
		return &bucketindex.Index{}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := rejectSeriesCatalogBytes(raw); err != nil {
		return nil, err
	}
	return bucketindex.Decode(raw)
}

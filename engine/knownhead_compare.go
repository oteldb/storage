package engine

import (
	"bytes"
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

const (
	knownCatalogBatchIDs   = 256
	knownCatalogBatchBytes = 64 << 10
	// Arena, ID arrays, interval-map growth and the target reader's pending-ID copy.
	knownCatalogCompareScratch = 128 << 10
)

type knownCanonicalBatch struct {
	ids    []signal.SeriesID
	data   []byte
	ranges map[signal.SeriesID][2]int
}

// compareKnownCatalog keeps source and target page readers bounded independently of catalog size.
func compareKnownCatalog(ctx context.Context, source, current *seriesSnapshot, ids []signal.SeriesID) error {
	b := knownCanonicalBatch{ids: make([]signal.SeriesID, 0, knownCatalogBatchIDs),
		data: make([]byte, 0, knownCatalogBatchBytes), ranges: make(map[signal.SeriesID][2]int, knownCatalogBatchIDs)}
	err := source.lookupKnownBatch(ctx, ids, func(id signal.SeriesID, raw []byte) error {
		if len(b.ids) == knownCatalogBatchIDs || len(b.data)+len(raw) > knownCatalogBatchBytes {
			if err := b.flush(ctx, current); err != nil {
				return err
			}
		}
		if len(raw) > knownCatalogBatchBytes {
			// A large identity stays borrowed while the target reader compares it; no large copy.
			return compareKnownBatch(ctx, current, []signal.SeriesID{id}, func(signal.SeriesID) []byte { return raw })
		}
		lo := len(b.data)
		b.data = append(b.data, raw...)
		b.ranges[id] = [2]int{lo, len(b.data)}
		b.ids = append(b.ids, id)
		return nil
	})
	if err != nil {
		return err
	}
	return b.flush(ctx, current)
}

func (b *knownCanonicalBatch) flush(ctx context.Context, current *seriesSnapshot) error {
	if len(b.ids) == 0 {
		return nil
	}
	err := compareKnownBatch(ctx, current, b.ids, func(id signal.SeriesID) []byte {
		bounds := b.ranges[id]
		return b.data[bounds[0]:bounds[1]]
	})
	if err != nil {
		return err
	}
	clear(b.ranges)
	b.ids = b.ids[:0]
	b.data = b.data[:0]
	return nil
}

func compareKnownBatch(ctx context.Context, current *seriesSnapshot, ids []signal.SeriesID, canonical func(signal.SeriesID) []byte) error {
	err := current.lookupKnownBatch(ctx, ids, func(id signal.SeriesID, raw []byte) error {
		if !bytes.Equal(raw, canonical(id)) {
			return errors.New("engine: known head canonical identity changed")
		}
		return nil
	})
	if errors.Is(err, errKnownIdentityMissing) {
		return errors.Wrap(err, "engine: current catalog lacks known head identity")
	}
	return err
}

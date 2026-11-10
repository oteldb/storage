package seriesindex

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"
)

func appendMetadata(dst []byte, meta Metadata) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(meta.Covered))
	if !meta.HasStats {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	dst = binary.BigEndian.AppendUint64(dst, uint64(meta.Stats.Count))
	for _, value := range [...]float64{meta.Stats.Sum, meta.Stats.Min, meta.Stats.Max} {
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func decodeMetadata(data []byte) (Metadata, []byte, error) {
	if len(data) < 9 {
		return Metadata{}, nil, errors.Wrap(ErrCorrupt, "short metadata")
	}
	meta := Metadata{Covered: int64(binary.BigEndian.Uint64(data))}
	flags := data[8]
	data = data[9:]
	if flags > 1 {
		return Metadata{}, nil, errors.Wrap(ErrCorrupt, "metadata flags")
	}
	if flags == 1 {
		if len(data) < 32 {
			return Metadata{}, nil, errors.Wrap(ErrCorrupt, "short stats")
		}
		meta.HasStats = true
		meta.Stats = Stats{Count: int64(binary.BigEndian.Uint64(data)),
			Sum: math.Float64frombits(binary.BigEndian.Uint64(data[8:])),
			Min: math.Float64frombits(binary.BigEndian.Uint64(data[16:])),
			Max: math.Float64frombits(binary.BigEndian.Uint64(data[24:]))}
		data = data[32:]
	}
	return meta, data, nil
}

func decodeIDValue(data []byte) (ref uint32, meta Metadata, inline bool, err error) {
	if len(data) < 4 {
		return 0, Metadata{}, false, errors.Wrap(ErrCorrupt, "short ID reference")
	}
	ref = binary.BigEndian.Uint32(data)
	if len(data) == 4 {
		return ref, Metadata{}, false, nil
	}
	meta, remaining, err := decodeMetadata(data[4:])
	if err != nil {
		return 0, Metadata{}, false, err
	}
	if len(remaining) != 0 {
		return 0, Metadata{}, false, errors.Wrap(ErrCorrupt, "trailing ID metadata")
	}
	return ref, meta, true, nil
}

func equalMetadata(a, b Metadata) bool {
	if a.Covered != b.Covered || a.HasStats != b.HasStats {
		return false
	}
	if !a.HasStats {
		return true
	}
	return a.Stats.Count == b.Stats.Count && math.Float64bits(a.Stats.Sum) == math.Float64bits(b.Stats.Sum) &&
		math.Float64bits(a.Stats.Min) == math.Float64bits(b.Stats.Min) && math.Float64bits(a.Stats.Max) == math.Float64bits(b.Stats.Max)
}

// validateReferences caches only successful ordinal validation; cancellation and I/O errors can retry.
func (r *Index) validateReferences(ctx context.Context) error {
	if r.referencesChecked.Load() {
		return nil
	}
	r.referencesMu.Lock()
	defer r.referencesMu.Unlock()
	if r.referencesChecked.Load() {
		return nil
	}
	ids := r.IDs(ctx)
	for ids.Next() {
	}
	return ids.Err()
}

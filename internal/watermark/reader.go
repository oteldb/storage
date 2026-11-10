package watermark

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

// ErrUnbounded means a backend cannot validate the sidecar through bounded reads.
var ErrUnbounded = errors.New("watermark: bounded reads unavailable")

// Reader retains validated framing, without retaining per-series entries.
type Reader struct {
	be     backend.Backend
	key    string
	offset int64
	count  int
}

// OpenReader validates the checksum and sorted IDs with at most 32 KiB of scratch.
func OpenReader(ctx context.Context, b backend.Backend, key string, count int) (*Reader, error) {
	size, known, err := backend.NativeSizeOf(ctx, b, key)
	if err != nil {
		return nil, err
	}
	if !known || !backend.RangesNatively(ctx, b, key) {
		return nil, ErrUnbounded
	}
	header, err := backend.ReadAt(ctx, b, key, 0, 4+binary.MaxVarintLen64)
	if err != nil {
		return nil, err
	}
	if len(header) < 9 || binary.BigEndian.Uint32(header) != magic {
		return nil, errors.Wrap(ErrCorrupt, "header")
	}
	n, m := binary.Uvarint(header[4:])
	if m <= 0 || n != uint64(count) {
		return nil, errors.Wrap(ErrCorrupt, "count")
	}
	offset := 4 + m
	if n > uint64((math.MaxInt64-int64(offset)-4)/entryWidth) || int64(offset)+int64(n)*entryWidth+4 != size {
		return nil, errors.Wrap(ErrCorrupt, "length")
	}
	checksum := crc32.Checksum(header[:offset], crcTable)
	var data []byte
	var prior signal.SeriesID
	for pos := 0; pos < count; {
		batch := min((32<<10)/entryWidth, count-pos)
		data, err = backend.ReadAtAppend(ctx, b, data[:0], key, int64(offset+pos*entryWidth), int64(batch*entryWidth))
		if err != nil {
			return nil, err
		}
		if len(data) != batch*entryWidth {
			return nil, errors.Wrap(ErrCorrupt, "short entries")
		}
		checksum = crc32.Update(checksum, crcTable, data)
		for i := range batch {
			raw := data[i*entryWidth:]
			id := signal.SeriesID{Hi: binary.BigEndian.Uint64(raw), Lo: binary.BigEndian.Uint64(raw[8:])}
			if pos+i > 0 && prior.Compare(id) >= 0 {
				return nil, errors.Wrap(ErrCorrupt, "unordered IDs")
			}
			prior = id
		}
		pos += batch
	}
	tail, err := backend.ReadAt(ctx, b, key, size-4, 4)
	if err != nil {
		return nil, err
	}
	if len(tail) != 4 || binary.BigEndian.Uint32(tail) != checksum {
		return nil, errors.Wrap(ErrCorrupt, "crc")
	}
	return &Reader{be: b, key: key, offset: int64(offset), count: count}, nil
}

// Lookup reads one series' covered timestamp without decoding other entries.
func (r *Reader) Lookup(ctx context.Context, id signal.SeriesID) (int64, bool, error) {
	lo, hi := 0, r.count
	var data []byte
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		var err error
		data, err = backend.ReadAtAppend(ctx, r.be, data[:0], r.key, r.offset+int64(mid*entryWidth), entryWidth)
		if err != nil {
			return 0, false, err
		}
		if len(data) != entryWidth {
			return 0, false, errors.Wrap(ErrCorrupt, "short lookup")
		}
		got := signal.SeriesID{Hi: binary.BigEndian.Uint64(data), Lo: binary.BigEndian.Uint64(data[8:])}
		switch got.Compare(id) {
		case -1:
			lo = mid + 1
		case 0:
			return int64(binary.BigEndian.Uint64(data[16:])), true, nil
		default:
			hi = mid
		}
	}
	return 0, false, nil
}

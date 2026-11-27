package watermark

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/sidecar"
	"github.com/oteldb/storage/signal"
)

// OrdinalMagic identifies a watermark sidecar whose IDs come from the associated SIDX.
const OrdinalMagic = 0x534d414f // SMAO.

func openOrdinal(
	ctx context.Context, b backend.Backend, key string, count int, size int64, binding [32]byte, header []byte,
) (*Reader, error) {
	const overhead = 4 + sidecar.OrdinalFooterBytes
	if count < 0 || uint64(count) > (math.MaxInt64-overhead)/8 || size != int64(count)*8+overhead {
		return nil, errors.Wrap(ErrCorrupt, "ordinal length")
	}
	end := size - sidecar.OrdinalFooterBytes
	checksum := crc32.Checksum(header, crcTable)
	var page []byte
	for off := int64(4); off < end; {
		n := min(int64(readerWindowBytes), end-off)
		var err error
		page, err = backend.ReadAtAppend(ctx, b, page[:0], key, off, n)
		if err != nil {
			return nil, err
		}
		if int64(len(page)) != n {
			return nil, errors.Wrap(ErrCorrupt, "short ordinal page")
		}
		checksum = crc32.Update(checksum, crcTable, page)
		off += n
	}
	footer, err := backend.ReadAt(ctx, b, key, end, sidecar.OrdinalFooterBytes)
	if err != nil {
		return nil, err
	}
	if err := sidecar.CheckOrdinalFooter(footer, binding, count, checksum); err != nil {
		return nil, errors.Wrap(ErrCorrupt, "ordinal footer")
	}
	return &Reader{be: b, key: key, offset: 4, count: count, ordinal: true}, nil
}

// Ordinal reports whether the reader uses only positions from SIDX.
func (r *Reader) Ordinal() bool { return r.ordinal }

// LookupSeries reads a coverage timestamp given its ID and known SIDX position.
func (r *Reader) LookupSeries(ctx context.Context, id signal.SeriesID, pos int) (int64, bool, error) {
	if !r.ordinal {
		return r.Lookup(ctx, id)
	}
	if pos < 0 || pos >= r.count {
		return 0, false, errors.Wrap(ErrCorrupt, "ordinal out of range")
	}
	var scratch [8]byte
	raw, err := backend.ReadAtAppend(ctx, r.be, scratch[:0], r.key, r.offset+int64(pos)*8, 8)
	if err != nil {
		return 0, false, err
	}
	if len(raw) != 8 {
		return 0, false, errors.Wrap(ErrCorrupt, "short ordinal")
	}
	return int64(binary.BigEndian.Uint64(raw)), true, nil
}

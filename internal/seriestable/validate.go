package seriestable

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

const validationBuffer = 64 << 10

func (r *Run) validate(ctx context.Context, be backend.Backend, key string, desc block.ColumnDesc, writtenMax int64) (err error) {
	r.idFilter = r.idCache.reserveFilter(r.Count())
	retained := r.idCache.reserve(r)
	defer func() {
		if err != nil {
			r.removeCachedIDs()
		}
	}()
	return r.validateContents(ctx, be, key, desc, retained, writtenMax)
}

func (r *Run) validateContents(ctx context.Context, be backend.Backend, key string, desc block.ColumnDesc,
	retained *cachedIDs, writtenMax int64,
) error {
	// Descriptor materialization is bounded by one page, never by run cardinality.
	n := 20*r.MaxPageBytes() + 4*validationBuffer
	budget := readbudget.From(ctx)
	if err := budget.Reserve(n); err != nil {
		return err
	}
	defer budget.Release(n)
	ctx = r.ReaderContext(ctx)
	reader, crc, size, err := r.openIDColumn(ctx, be, key, desc)
	if err != nil {
		return err
	}
	var descriptors descriptorValidator
	if writtenMax == 0 {
		descriptors.reader = r.index.RawReader(ctx)
		descriptors.reader.StreamPages()
	}
	r.maxCanonicalBytes = writtenMax
	var encoded [17]byte
	var seen uint32
	if err := r.index.VisitIDs(ctx, func(indexID signal.SeriesID, ref uint32, inline seriesindex.Metadata) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return errors.Wrap(err, "read ID column")
		}
		id := signal.SeriesID{Hi: binary.BigEndian.Uint64(encoded[:8]), Lo: binary.BigEndian.Uint64(encoded[8:16])}
		if encoded[16] != 1 || id != indexID {
			return errors.Wrap(ErrCorrupt, "ID column and index disagree")
		}
		if inline != (seriesindex.Metadata{}) {
			return errors.Wrap(ErrCorrupt, "descriptor sample metadata")
		}
		if ref != seen {
			return errors.Wrap(ErrCorrupt, "noncanonical descriptor ordinal")
		}
		if descriptors.reader != nil {
			size, err := descriptors.check(id, ref)
			if err != nil {
				return err
			}
			r.maxCanonicalBytes = max(r.maxCanonicalBytes, size)
		}
		retained.set(seen, id)
		if r.idFilter != nil {
			r.idFilter.add(id.Lo)
		}
		seen++
		return nil
	}); err != nil {
		return err
	}
	if seen != r.Count() {
		return errors.Wrap(ErrCorrupt, "descriptor count")
	}
	if _, err := reader.ReadByte(); err == nil {
		return errors.Wrap(ErrCorrupt, "trailing ID bytes")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	trailer, err := backend.ReadAt(ctx, be, key, size-4, 4)
	if err != nil {
		return err
	}
	if len(trailer) != 4 || binary.LittleEndian.Uint32(trailer) != crc.Sum32() {
		return errors.Wrap(ErrCorrupt, "ID checksum")
	}
	r.idCache.publish(r)
	return nil
}

type descriptorValidator struct {
	reader    *seriesindex.RawReader
	identity  signal.Series
	canonical []byte
}

func (v *descriptorValidator) check(id signal.SeriesID, ref uint32) (int64, error) {
	raw, meta, err := v.reader.LookupOrdinal(id, ref)
	if err != nil {
		return 0, err
	}
	if meta != (seriesindex.Metadata{}) {
		return 0, errors.Wrap(ErrCorrupt, "descriptor sample metadata")
	}
	used, err := signal.DecodeSeriesInto(&v.identity, raw)
	if err != nil || used != len(raw) {
		return 0, errors.Wrap(ErrCorrupt, "canonical descriptor")
	}
	v.canonical = v.identity.AppendHashInput(v.canonical[:0])
	if !bytes.Equal(raw, v.canonical) {
		return 0, errors.Wrap(ErrCorrupt, "noncanonical descriptor")
	}
	return int64(len(raw)), nil
}

func (r *Run) openIDColumn(
	ctx context.Context, be backend.Backend, key string, desc block.ColumnDesc,
) (*bufio.Reader, hash.Hash32, int64, error) {
	size, err := requireObject(ctx, be, key, 0)
	if err != nil {
		return nil, nil, 0, err
	}
	var header [binary.MaxVarintLen64]byte
	headerLen := binary.PutUvarint(header[:], uint64(r.Count()))
	streamBytes := int64(headerLen) + int64(r.Count())*17
	if size != streamBytes+5 || desc.Bytes != size || desc.Sizing.StreamRaw != streamBytes {
		return nil, nil, 0, errors.Wrap(ErrCorrupt, "ID column length")
	}
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	reader := bufio.NewReaderSize(io.TeeReader(&rangeReader{ctx: ctx, be: be, key: key, end: size - 4}, crc), validationBuffer)
	flag, err := reader.ReadByte()
	if err != nil {
		return nil, nil, 0, err
	}
	if flag != compress.FlagRaw {
		return nil, nil, 0, errors.Wrap(ErrCorrupt, "ID compression flag")
	}
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, nil, 0, err
	}
	if count != uint64(r.Count()) {
		return nil, nil, 0, errors.Wrap(ErrCorrupt, "ID column count")
	}
	return reader, crc, size, nil
}

type rangeReader struct {
	ctx         context.Context //nolint:containedctx // Scoped adapter for bounded ID-column validation.
	be          backend.Backend
	key         string
	offset, end int64
}

func (r *rangeReader) Read(dst []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.offset == r.end {
		return 0, io.EOF
	}
	n := min(int64(len(dst)), r.end-r.offset)
	data, err := backend.ReadAt(r.ctx, r.be, r.key, r.offset, n)
	if err != nil {
		return 0, err
	}
	if int64(len(data)) != n {
		return 0, io.ErrUnexpectedEOF
	}
	copy(dst, data)
	r.offset += n
	return int(n), nil
}

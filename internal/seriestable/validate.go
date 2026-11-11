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

func (r *Run) validate(ctx context.Context, be backend.Backend, key string, desc block.ColumnDesc) error {
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
	ids := r.index.IDs(ctx)
	rawReader := r.index.RawReader(ctx)
	var identity signal.Series
	var canonical []byte
	var encoded [17]byte
	var seen uint32
	for ids.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, encoded[:]); err != nil {
			return errors.Wrap(err, "read ID column")
		}
		id := signal.SeriesID{Hi: binary.BigEndian.Uint64(encoded[:8]), Lo: binary.BigEndian.Uint64(encoded[8:16])}
		if encoded[16] != 1 || id != ids.At() {
			return errors.Wrap(ErrCorrupt, "ID column and index disagree")
		}
		raw, meta, found, err := rawReader.Lookup(id)
		if err != nil {
			return err
		}
		if !found || meta != (seriesindex.Metadata{}) {
			return errors.Wrap(ErrCorrupt, "descriptor sample metadata")
		}
		used, err := signal.DecodeSeriesInto(&identity, raw)
		if err != nil || used != len(raw) {
			return errors.Wrap(ErrCorrupt, "canonical descriptor")
		}
		canonical = identity.AppendHashInput(canonical[:0])
		if !bytes.Equal(raw, canonical) {
			return errors.Wrap(ErrCorrupt, "noncanonical descriptor")
		}
		seen++
	}
	if err := ids.Err(); err != nil {
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
	return nil
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

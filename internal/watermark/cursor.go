package watermark

import (
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

const cursorEntries = readerWindowBytes / entryWidth

// Cursor reuses one bounded window of a validated immutable watermark sidecar.
type Cursor struct {
	reader *Reader
	ctx    context.Context //nolint:containedctx // Operation-local cursor owns the ranged-read lifetime.
	first  int
	data   []byte
}

// Cursor creates an operation-local reader with at most 32 KiB of scratch.
func (r *Reader) Cursor(ctx context.Context) *Cursor {
	c := &Cursor{}
	c.Reset(ctx, r)
	return c
}

// Reset rebinds an operation-local window while retaining its scratch capacity.
func (c *Cursor) Reset(ctx context.Context, r *Reader) {
	c.reader, c.ctx, c.first = r, ctx, -1
	c.data = c.data[:0]
}

// At reads an ordinal, retaining its page for subsequent ordinals.
func (c *Cursor) At(index int) (Entry, error) {
	if c.reader.ordinal {
		return Entry{}, errors.Wrap(ErrCorrupt, "ordinal cursor requires SIDX ID")
	}
	return c.AtSeries(index, signal.SeriesID{})
}

// AtSeries reads a watermark at the ordinal of id in the bound SIDX.
func (c *Cursor) AtSeries(index int, id signal.SeriesID) (Entry, error) {
	if index < 0 || index >= c.reader.count {
		return Entry{}, errors.Wrap(ErrCorrupt, "watermark ordinal out of range")
	}
	width := entryWidth
	if c.reader.ordinal {
		width = 8
	}
	windowEntries := readerWindowBytes / width
	first := index / windowEntries * windowEntries
	if c.first != first {
		count := min(windowEntries, c.reader.count-first)
		c.first = -1
		data, err := backend.ReadAtAppend(c.ctx, c.reader.be, c.data[:0], c.reader.key,
			c.reader.offset+int64(first*width), int64(count*width))
		if err != nil {
			return Entry{}, err
		}
		if len(data) != count*width {
			return Entry{}, errors.Wrap(ErrCorrupt, "short watermark window")
		}
		c.data, c.first = data, first
	}
	raw := c.data[(index-c.first)*width:]
	if c.reader.ordinal {
		return Entry{ID: id, Max: int64(binary.BigEndian.Uint64(raw))}, nil
	}
	return Entry{
		ID:  signal.SeriesID{Hi: binary.BigEndian.Uint64(raw), Lo: binary.BigEndian.Uint64(raw[8:])},
		Max: int64(binary.BigEndian.Uint64(raw[16:])),
	}, nil
}

package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
)

// errNonContiguousDecode is returned when a streaming merge requests a series range that is not
// contiguous with the part cursor's position — an invariant violation (rows are sorted by series,
// so a sorted merge advances each part's cursor monotonically through contiguous ranges).
var errNonContiguousDecode = errors.New("engine: non-contiguous streaming decode")

// defaultMergeReadWindow is how much of each source column a merge reads ahead, and so the read
// side's resident term: sources × columns × window, whatever the sources' size.
const defaultMergeReadWindow = 1 << 20

// partStream is a per-merge forward cursor over a part's (series-sorted) ts/value(/sf) columns. It
// decodes one series range at a time, advancing strictly forward through the part's rows. Because
// rows are sorted by (series, ts) and the merge visits series in ascending order, a part's series
// ranges are contiguous and the cursors move monotonically — each column is read and decoded exactly
// once across the merge, one read-ahead window at a time.
type partStream struct {
	ts  chunk.TsCursor
	val chunk.FloatDecoder
	sf  chunk.FloatDecoder // nil when the part has no scale-factor column
	pos int                // next undecoded row; a requested range's start must equal pos
}

// newPartStream opens forward cursors over p's ts/value(/sf) columns, each reading window bytes of
// its encoded column ahead.
func newPartStream(ctx context.Context, p *part, window int64) (*partStream, error) {
	ts, err := scanColumn(ctx, p.reader, colTs, window,
		(*block.ColumnReader).TsCursor, (*block.Decoder).TsCursor)
	if err != nil {
		return nil, err
	}

	val, err := scanColumn(ctx, p.reader, colValue, window,
		(*block.ColumnReader).FloatCursor, (*block.Decoder).FloatCursor)
	if err != nil {
		return nil, err
	}

	var sf chunk.FloatDecoder

	if p.hasSF {
		if sf, err = scanColumn(ctx, p.reader, colSF, window,
			(*block.ColumnReader).FloatCursor, (*block.Decoder).FloatCursor); err != nil {
			return nil, err
		}
	}

	return &partStream{ts: ts, val: val, sf: sf}, nil
}

// scanColumn opens a forward cursor over the named column. A constant or unblocked column has no
// frames to window over — the first costs no I/O, the second is one small stream — so it takes the
// whole-object reader.
func scanColumn[C any](
	ctx context.Context, r *block.PartReader, name string, window int64,
	whole func(*block.ColumnReader) (C, error), scan func(*block.Decoder) (C, error),
) (C, error) {
	var zero C

	if desc, ok := r.ColumnDescByName(name); ok && (desc.Const || !desc.Blocked) {
		col, err := r.Column(ctx, name)
		if err != nil {
			return zero, err
		}

		return whole(col)
	}

	d, err := r.ColumnScan(ctx, name, window)
	if err != nil {
		return zero, errors.Wrapf(err, "scan column %q", name)
	}

	return scan(d)
}

// rangeBuf is a reusable per-part destination for one series' decoded range, recycled across the
// series of a merge so the streaming path allocates only as the ranges grow.
type rangeBuf struct {
	ts   []int64
	vals []float64
	sf   []float64
}

// decodeRange decodes the part's rows [rng.start, rng.end) into the reusable destination buffers
// (growing them as needed) and returns the populated slices. The cursors must be positioned at
// rng.start (contiguous with the previous range); decodeRange advances them to rng.end.
func (s *partStream) decodeRange(rng rowRange, dst *rangeBuf) (ts []int64, vals, sf []float64, _ error) {
	n := rng.end - rng.start
	if n == 0 {
		return nil, nil, nil, nil
	}

	if rng.start != s.pos {
		return nil, nil, nil, errors.Wrapf(errNonContiguousDecode, "at row %d, cursor at %d", rng.start, s.pos)
	}

	dst.ts = growLen(dst.ts, n)
	dst.vals = growLen(dst.vals, n)

	for i := range n {
		v, err := s.ts.Next()
		if err != nil {
			return nil, nil, nil, err
		}

		dst.ts[i] = v
	}

	for i := range n {
		v, err := s.val.Next()
		if err != nil {
			return nil, nil, nil, err
		}

		dst.vals[i] = v
	}

	var sfOut []float64

	if s.sf != nil {
		dst.sf = growLen(dst.sf, n)

		for i := range n {
			v, err := s.sf.Next()
			if err != nil {
				return nil, nil, nil, err
			}

			dst.sf[i] = v
		}

		sfOut = dst.sf[:n]
	}

	s.pos = rng.end

	return dst.ts[:n], dst.vals[:n], sfOut, nil
}

// growLen returns a slice of length n, reusing dst's backing array when its capacity allows and
// allocating otherwise.
func growLen[T any](dst []T, n int) []T {
	if cap(dst) >= n {
		return dst[:n]
	}

	return make([]T, n)
}

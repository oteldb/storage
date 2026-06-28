package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
)

// errNonContiguousDecode is returned when a streaming merge requests a series range that is not
// contiguous with the part cursor's position — an invariant violation (rows are sorted by series,
// so a sorted merge advances each part's cursor monotonically through contiguous ranges).
var errNonContiguousDecode = errors.New("engine: non-contiguous streaming decode")

// partStream is a per-merge forward cursor over a part's (series-sorted) ts/value(/sf) columns. It
// decodes one series range at a time, advancing strictly forward through the part's rows, so a
// streaming merge holds only the current range resident per source part instead of the part's whole
// decoded column (issue #25, item 1's full fix). Because rows are sorted by (series, ts) and the
// merge visits series in ascending order, a part's series ranges are contiguous and the cursors move
// monotonically — each part's column is decoded exactly once across the merge.
type partStream struct {
	ts  chunk.TsCursor
	val chunk.FloatDecoder
	sf  chunk.FloatDecoder // nil when the part has no scale-factor column
	pos int                // next undecoded row; a requested range's start must equal pos
}

// newPartStream opens forward cursors over p's ts/value(/sf) columns.
func newPartStream(ctx context.Context, p *part) (*partStream, error) {
	tsCol, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return nil, err
	}

	ts, err := tsCol.TsCursor()
	if err != nil {
		return nil, err
	}

	valCol, err := p.reader.Column(ctx, colValue)
	if err != nil {
		return nil, err
	}

	val, err := valCol.FloatCursor()
	if err != nil {
		return nil, err
	}

	var sf chunk.FloatDecoder

	if p.hasSF {
		sfCol, err := p.reader.Column(ctx, colSF)
		if err != nil {
			return nil, err
		}

		if sf, err = sfCol.FloatCursor(); err != nil {
			return nil, err
		}
	}

	return &partStream{ts: ts, val: val, sf: sf}, nil
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

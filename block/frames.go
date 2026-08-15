package block

// FrameExtent is one compression frame of a column: the row span it covers and the compressed byte
// span it occupies in the column object.
type FrameExtent struct {
	StartRow int   // inclusive
	EndRow   int   // exclusive
	Bytes    int64 // compressed size of the frame
}

// Frames maps the column's rows onto its compression frames. A frame is the unit compression is
// applied to, so it is the finest granularity at which a caller can attribute a column's
// *compressed* bytes to a subset of its rows — nothing below it is separable, since the entropy
// coder shares state across the whole frame.
//
// A column that is not block-framed (constant, or written as one stream) reports a single extent
// covering every row and the whole object. The extents' Bytes sum to the frames' bytes, which is
// less than [ColumnReader.ObjectBytes] by the directory (and, for a shared-dictionary column, by
// the dictionary) — neither belongs to any single frame.
func (r *ColumnReader) Frames() ([]FrameExtent, error) {
	if r.rows == 0 {
		return nil, nil
	}

	if !r.desc.Blocked || r.desc.Const {
		return []FrameExtent{{StartRow: 0, EndRow: r.rows, Bytes: int64(len(r.object))}}, nil
	}

	d, err := r.blockDir()
	if err != nil {
		return nil, err
	}

	if d.granules == 0 || d.blockRows <= 0 {
		return []FrameExtent{{StartRow: 0, EndRow: r.rows, Bytes: int64(len(r.object))}}, nil
	}

	out := make([]FrameExtent, 0, len(d.frameOff))

	for g := 0; g < d.granules; {
		f := d.frameOf(g)

		h := g + 1
		for h < d.granules && d.frameOf(h) == f {
			h++
		}

		out = append(out, FrameExtent{
			StartRow: min(g*d.blockRows, r.rows),
			EndRow:   min(h*d.blockRows, r.rows),
			Bytes:    int64(d.frameOff[f+1] - d.frameOff[f]),
		})

		g = h
	}

	return out, nil
}

// ObjectBytes is the column object's size as stored on the backend, 0 for a constant column (which
// has no object).
func (r *ColumnReader) ObjectBytes() int64 { return int64(len(r.object)) }

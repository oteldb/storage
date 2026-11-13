package series

import (
	"github.com/oteldb/storage/signal"
)

// AddChecked registers s only when its canonical hash matches expected.
// It returns the canonical byte length; only scratch may change on a mismatch.
func (ix *Index) AddChecked(expected signal.SeriesID, s signal.Series) (int, bool) {
	ix.buf = s.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)
	if id != expected {
		return len(ix.buf), false
	}
	ix.AddEntry(Entry{ID: id, Series: s})
	return len(ix.buf), true
}

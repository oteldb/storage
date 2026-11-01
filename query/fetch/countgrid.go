package fetch

import (
	"context"

	"github.com/go-faster/errors"
)

// WindowGrid describes NumSteps windows (Start+i*Step-Width, Start+i*Step], in nanoseconds.
// Step, Width and NumSteps must be positive, and the last endpoint must fit int64.
type WindowGrid struct {
	Start, Step, Width int64
	NumSteps           int
}

// ErrCountGridUnsupported asks an adapter to retain its ordinary aggregate fallback.
var ErrCountGridUnsupported = errors.New("count grid unsupported")

// GridCounter counts series present at each grid step, optionally grouped by one label's text.
// A nil groupBy produces a single empty-string key. Zero-only groups are omitted. Samples must
// also lie in Request's inclusive bounds; duplicates and sampling weights do not multiply presence.
type GridCounter interface {
	CountGrid(ctx context.Context, r Request, grid WindowGrid, groupBy []byte) (map[string][]int, error)
}

// GridCounterOf finds the first grid-count capability through single-child decorators.
func GridCounterOf(f Fetcher) GridCounter {
	for f != nil {
		if c, ok := f.(GridCounter); ok {
			return c
		}
		u, ok := f.(Unwraper)
		if !ok {
			return nil
		}
		f = u.Unwrap()
	}
	return nil
}

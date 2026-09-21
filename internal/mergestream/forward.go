package mergestream

import "github.com/go-faster/errors"

// ErrNotForward reports a row range a forward-only cursor cannot serve.
var ErrNotForward = errors.New("row range cannot be read forward-only")

// CheckForward verifies that the row range [start, end) can be read by a forward-only cursor that
// has already consumed every row below pos. Skipping forward is fine — the cursor discards the gap
// — but a range that begins behind pos, or ends before it begins, needs a reader that can seek
// backwards and so fails with an error wrapping [ErrNotForward].
func CheckForward(pos, start, end int) error {
	if start < pos || end < start {
		return errors.Wrapf(ErrNotForward, "range [%d,%d) behind cursor at %d", start, end, pos)
	}

	return nil
}

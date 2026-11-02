package fetch

import (
	"context"
	"errors"
)

// ErrDecodeBusy means a nonblocking read could not reserve decode memory. No iterator or reservation is retained.
var ErrDecodeBusy = errors.New("decode budget busy")

// ErrTryUnsupported means a read path cannot guarantee nonblocking decode admission.
var ErrTryUnsupported = errors.New("nonblocking decode admission unsupported")

// TryFetcher optionally admits a fetch without waiting for decode memory. It preserves Fetch's
// results and iterator ownership; contention returns [ErrDecodeBusy], and unsupported paths return
// [ErrTryUnsupported]. Nonblocking refers to decode admission, not engine locks or backend I/O.
// An already-reserved Scope keeps its ordinary growth accounting and admit-alone semantics apply.
// Callers must not borrow another query's Scope for an independently owned producer.
type TryFetcher interface {
	TryFetch(ctx context.Context, r Request) (Iterator, error)
}

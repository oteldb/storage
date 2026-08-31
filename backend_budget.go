package storage

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/readbudget"
)

// The backend charge point: what a query pulls off disk or out of an object store, denominated in
// the same resident bytes as every other layer.
//
// It rides the metering decorator rather than being a decorator of its own, because that one already
// forwards every optional backend capability correctly — a second wrapper that forgot one would
// silently turn a ranged column read back into a whole-object read, which the [backend.ReaderAt] doc
// calls out as the cost of exactly that mistake.
//
// Charges here are not released. They do not need to be: a part's decoded columns stay pinned for
// the life of the fetch that opened them (the metric engine's iterator holds them until Close), so
// "everything this fetch read" *is* what it holds, and the budget is discarded when the fetch ends.

// preflightRead refuses a read whose size is known before a byte of it is allocated, which is the
// reason to charge at the backend at all rather than only counting what comes out the other end.
// A size ≤ 0 means unknown and preflights nothing; the post-read reservation still applies.
func preflightRead(ctx context.Context, key string, size int64) error {
	b := readbudget.From(ctx)
	if b == nil || size <= 0 {
		return nil
	}

	if remaining := b.Remaining(); size > remaining {
		return errors.Wrapf(readbudget.ErrExceeded,
			"read %q of %d bytes: query may hold %d more", key, size, remaining)
	}

	return nil
}

// chargeRead reserves what a completed read now holds. It takes readErr so a caller can wrap a read
// in one statement: a failed read charges nothing and reports its own error.
func chargeRead(ctx context.Context, data []byte, readErr error) ([]byte, error) {
	if readErr != nil {
		return data, readErr
	}

	if err := readbudget.From(ctx).Reserve(int64(len(data))); err != nil {
		return nil, err
	}

	return data, nil
}

// aliases reports whether a view read returns backend-owned memory rather than a fresh copy. An
// aliased view is not charged: the bytes are the memory backend's own map entry or a read-cache
// entry, resident whether or not this query ran and bounded by that cache's own size. Charging them
// would fail a query that allocated nothing — the worst kind of false positive for a bound whose
// whole job is to be invisible until something is genuinely too big.
func aliases(inner backend.Backend, ranged bool) bool {
	if ranged {
		_, ok := inner.(backend.ViewerAt)

		return ok
	}

	_, ok := inner.(backend.Viewer)

	return ok
}

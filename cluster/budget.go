package cluster

import (
	"context"
	"io"
	"net/http"
	"strconv"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/readbudget"
)

// budgetHeader carries the caller's remaining memory allowance to a peer, so the peer stops early
// rather than serializing a response the caller has no room to accept.
//
// It is what makes this one budget enforced at several points rather than two independent limits
// that happen to be configured alike. Both ends deliberately use the *same* number: the allowance
// belongs to the query's answer, not to the cluster's shape. Giving each of N peers its own full
// allowance would let the aggregator's real ceiling grow with the node count — adding nodes would
// raise the peak heap one process can be driven to by a single query, which is backwards.
const budgetHeader = "X-Oteldb-Read-Budget"

// sendBudget declares the caller's remaining allowance on an outgoing fan-out request. A read with
// no budget declares nothing, and the peer treats it as unbounded exactly as a local read would.
func sendBudget(ctx context.Context, h http.Header) {
	if b := readbudget.From(ctx); b != nil {
		h.Set(budgetHeader, strconv.FormatInt(b.Remaining(), 10))
	}
}

// recvBudget returns ctx carrying the allowance the caller declared, so the peer's own read is
// bounded by what the caller can still hold. A missing, malformed, or non-positive header leaves ctx
// untouched: a peer must not invent a bound the caller did not ask for.
func recvBudget(ctx context.Context, h http.Header) context.Context {
	raw := h.Get(budgetHeader)
	if raw == "" {
		return ctx
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return ctx
	}

	return readbudget.With(ctx, readbudget.New(n))
}

// readBudgetedBody reads a fan-out response body under the query's memory budget.
//
// It refuses before allocating when the declared length already exceeds what the query may hold —
// the point of charging here rather than downstream is that the bytes are never committed — and it
// caps the read regardless, so an absent or dishonest Content-Length cannot get past it either.
//
// The returned release must be called once the body has been decoded. The wire bytes are transient:
// they expand into batches that are charged in their own right, so holding both reservations past
// the decode would charge one query twice for the same data.
func readBudgetedBody(ctx context.Context, body io.Reader, length int64) (_ []byte, release func(), _ error) {
	budget := readbudget.From(ctx)
	if budget == nil {
		data, err := io.ReadAll(body)

		return data, func() {}, err
	}

	remaining := budget.Remaining()
	if length > remaining {
		return nil, nil, errors.Wrapf(readbudget.ErrExceeded,
			"response of %d bytes: query may hold %d more", length, remaining)
	}

	// One byte past the allowance is enough to tell "exactly at the limit" from "over it" without
	// reading a body we have already decided to refuse.
	data, err := io.ReadAll(io.LimitReader(body, remaining+1))
	if err != nil {
		return nil, nil, errors.Wrap(err, "read response")
	}

	if int64(len(data)) > remaining {
		return nil, nil, errors.Wrapf(readbudget.ErrExceeded,
			"response exceeds the %d bytes the query may still hold", remaining)
	}

	n := int64(len(data))
	if err := budget.Reserve(n); err != nil {
		return nil, nil, err
	}

	return data, func() { budget.Release(n) }, nil
}

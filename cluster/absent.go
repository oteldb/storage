package cluster

import (
	"bytes"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
)

// ErrShardAbsent is a peer's answer that it does not hold the requested shard at all: it has no
// engine for the shard, whatever the ring says (a fresh owner after a rebalance, a node whose
// membership view lags the writer's, a spare that has not backfilled yet).
//
// It is deliberately not an empty result. An empty success is indistinguishable from "this shard
// really has no data", so a caller that accepts it silently drops every row the shard holds
// elsewhere; the caller must instead fail over to another owner, and report empty only when every
// owner disclaims the shard.
var ErrShardAbsent = errors.New("cluster: shard not held by peer")

// absentStatus is how [ErrShardAbsent] crosses the wire. Deliberately not 404: a peer too old to
// know an endpoint answers 404 from its mux, and that must keep failing over (or falling back to a
// non-pushdown path) rather than being read as "this shard has no data".
const absentStatus = http.StatusConflict

// budgetStatus is how [readbudget.ErrExceeded] crosses the wire. Without it [statusError] flattens
// the sentinel into a message, and an embedder's errors.Is check — the one deciding between "your
// query is too wide" and "the server broke" — silently stops working the moment a read is served by
// a peer rather than locally.
//
// It must not be a code infrastructure produces on its own (a proxy's 502/503, a mux's 404), which
// mean something else entirely; 422 is not otherwise served here.
//
// Unlike [absentStatus] this is not a failover signal. Every owner holds the same data and would
// refuse the same query, so retrying elsewhere only spends the budget again.
const budgetStatus = http.StatusUnprocessableEntity

// unsupportedStatus is how [fetch.ErrLabelsUnsupported] crosses the wire: the peer holds the shard
// but the requested signal has no label index to answer from. It is not an absence — failing over to
// another owner would meet the same answer — and not a fault either: the caller stays on the path it
// was already taking.
const unsupportedStatus = http.StatusNotImplemented

// writeRPCError replies to a read/enumeration RPC, mapping [ErrShardAbsent] to [absentStatus] so
// the caller fails over to another owner instead of accepting an empty answer.
func writeRPCError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrShardAbsent):
		code = absentStatus
	case errors.Is(err, readbudget.ErrExceeded):
		code = budgetStatus
	case errors.Is(err, fetch.ErrLabelsUnsupported):
		code = unsupportedStatus
	}

	http.Error(w, err.Error(), code)
}

// statusError turns a non-200 RPC response into an error, preserving the sentinels a caller
// branches on: absence, so it fails over, and budget exhaustion, so it reports a client error
// rather than a server fault.
func statusError(addr, what string, code int, body []byte) error {
	switch {
	case code == absentStatus:
		return errors.Wrapf(ErrShardAbsent, "%q %s", addr, what)
	case code == budgetStatus:
		return errors.Wrapf(readbudget.ErrExceeded, "%q %s: %s", addr, what, bytes.TrimSpace(body))
	// A peer too old to know the label endpoint answers 404 from its mux. That is the same
	// situation as an explicit "no label index here", and must fall back rather than fail the
	// user's query, so it decodes to the same sentinel.
	case code == unsupportedStatus, code == http.StatusNotFound && what == LabelsPath:
		return errors.Wrapf(fetch.ErrLabelsUnsupported, "%q %s", addr, what)
	}

	return errors.Errorf("cluster: %q %s returned %d: %s", addr, what, code, bytes.TrimSpace(body))
}

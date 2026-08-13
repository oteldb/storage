package cluster

import (
	"bytes"
	"net/http"

	"github.com/go-faster/errors"
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

// writeRPCError replies to a read/enumeration RPC, mapping [ErrShardAbsent] to [absentStatus] so
// the caller fails over to another owner instead of accepting an empty answer.
func writeRPCError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, ErrShardAbsent) {
		code = absentStatus
	}

	http.Error(w, err.Error(), code)
}

// statusError turns a non-200 RPC response into an error, preserving the absence signal.
func statusError(addr, what string, code int, body []byte) error {
	if code == absentStatus {
		return errors.Wrapf(ErrShardAbsent, "%q %s", addr, what)
	}

	return errors.Errorf("cluster: %q %s returned %d: %s", addr, what, code, bytes.TrimSpace(body))
}

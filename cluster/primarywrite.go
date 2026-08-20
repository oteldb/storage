package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// ErrNotPrimary is a node's refusal to apply a write as a shard's primary: it can no longer prove
// it holds the shard, so another node may already own it and anything applied here would be
// acknowledged and then withheld. It is the write side of [ErrShardAbsent] — a routing answer, not
// a server fault — so the origin should re-resolve the shard's primary rather than retry here.
var ErrNotPrimary = errors.New("cluster: shard no longer held by this node")

// PrimaryWritePath is the endpoint a shard's ring primary serves: the single authority for the
// shard, so every write for it lands here and the admission decision is made once.
const PrimaryWritePath = "/internal/primary-write"

// Reject is the per-reason rejection breakdown a primary reports back to the write's origin, so
// ingest can attribute OTLP partial-success exactly like the single-node path. The rate valve is
// applied at the origin, so it is not carried here.
type Reject struct {
	OOO         int
	Cardinality int
	InFlight    int
}

// Total is how many records the primary rejected, for all reasons it reports.
func (r Reject) Total() int { return r.OOO + r.Cardinality + r.InFlight }

// PrimaryWriteFunc applies a write as the addressed shard's primary and returns what it rejected.
// walBytes is the WAL-encoded run of records for the shard, framed by [EncodeWrite].
type PrimaryWriteFunc func(ctx context.Context, sig signal.Signal, shardKey string, walBytes []byte) (Reject, error)

// PrimaryWriteHandler returns the HTTP handler serving writes routed to this node as a shard's
// primary. Mount it at [PrimaryWritePath].
func PrimaryWriteHandler(fn PrimaryWriteFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		payload, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		sig, shardKey, walBytes, err := DecodeWrite(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace

		rej, err := fn(ctx, sig, shardKey, walBytes)
		if err != nil {
			// A disclaimed shard is 409, like a read's ErrShardAbsent: the origin must be able
			// to tell "ask someone else" from "this node is broken".
			code := http.StatusInternalServerError
			if errors.Is(err, ErrNotPrimary) {
				code = absentStatus
			}

			http.Error(w, err.Error(), code)

			return
		}

		_, _ = fmt.Fprintf(w, "%d %d %d", rej.OOO, rej.Cardinality, rej.InFlight)
	})
}

// SendPrimaryWrite posts one already-framed write ([EncodeWrite]) to the primary at addr and
// returns the breakdown it reports. A nil client uses [http.DefaultClient].
//
// It makes exactly one attempt and never retries: a write is not idempotent, so re-sending one the
// primary may have applied is the caller's decision, not this function's. Callers that can prove
// the request never reached the server (a connection failure) may retry it themselves.
func SendPrimaryWrite(ctx context.Context, client *http.Client, addr string, payload []byte) (Reject, error) {
	if client == nil {
		client = http.DefaultClient
	}

	u := (&url.URL{Scheme: httpScheme, Host: addr}).JoinPath(PrimaryWritePath)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return Reject{}, errors.Wrap(err, "build primary-write request")
	}

	obs.InjectHTTP(ctx, req.Header) // carry the trace into the primary-write RPC

	resp, err := client.Do(req)
	if err != nil {
		return Reject{}, errors.Wrapf(err, "primary-write to %q", addr)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Reject{}, errors.Wrap(err, "read primary-write response")
	}

	if resp.StatusCode == absentStatus {
		return Reject{}, errors.Wrapf(ErrNotPrimary, "primary %q", addr)
	}

	if resp.StatusCode != http.StatusOK {
		return Reject{}, errors.Errorf("cluster: primary %q returned %d: %s", addr, resp.StatusCode, bytes.TrimSpace(body))
	}

	var rej Reject
	if _, err := fmt.Sscanf(string(bytes.TrimSpace(body)), "%d %d %d", &rej.OOO, &rej.Cardinality, &rej.InFlight); err != nil {
		return Reject{}, errors.Wrap(err, "parse reject breakdown")
	}

	return rej, nil
}

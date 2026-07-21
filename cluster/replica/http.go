package replica

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/obs"
)

const (
	// ReplicatePath is the HTTP path the replication server serves and the transport posts to.
	ReplicatePath = "/internal/replicate"
	httpScheme    = "http"
)

// httpTransport sends replicated writes to peers over HTTP POST.
type httpTransport struct {
	client *http.Client
}

// NewHTTPTransport returns a [Transport] that POSTs payloads to peers at
// http://{addr}{ReplicatePath}. A nil client uses [http.DefaultClient]; pass one with a
// timeout in production.
func NewHTTPTransport(client *http.Client) Transport {
	if client == nil {
		client = http.DefaultClient
	}

	return &httpTransport{client: client}
}

func (t *httpTransport) Send(ctx context.Context, addr string, payload []byte) error {
	u := (&url.URL{Scheme: httpScheme, Host: addr}).JoinPath(ReplicatePath)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	obs.InjectHTTP(ctx, req.Header) // carry the trace into the replication RPC

	resp, err := t.client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "post to %q", addr)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount of the body for the error message / connection reuse.
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

		return errors.Errorf("replica: %q returned %d: %s", addr, resp.StatusCode, bytes.TrimSpace(msg))
	}

	return nil
}

// Handler returns the HTTP handler that receives replicated writes and applies them via the
// replicator. Mount it on the node's internal server at [ReplicatePath]:
//
//	mux.Handle(replica.ReplicatePath, rp.Handler())
func (r *Replicator) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		payload, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)

			return
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace
		if err := r.apply(ctx, payload); err != nil {
			http.Error(w, "apply: "+err.Error(), http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

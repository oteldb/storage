package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// ReadPath is the HTTP path the cluster read (fetch fan-out) server serves.
const ReadPath = "/internal/fetch"

// The cluster read RPC carries only a tenant and a time window — not the fetch matchers, which
// are opaque Go predicates (not serializable). A peer returns every series in the window (a
// superset, which the fetch contract permits); the requesting node re-applies its matchers.

// EncodeFetchRequest frames a fetch request: tenant, window, and any serializable equality
// matchers to push down to the peer (other predicates are re-checked by the requester).
func EncodeFetchRequest(tenant string, start, end int64, eq []fetch.EqualMatcher) []byte {
	buf := appendString(nil, tenant)
	buf = binary.AppendVarint(buf, start)
	buf = binary.AppendVarint(buf, end)
	buf = binary.AppendUvarint(buf, uint64(len(eq)))
	for _, m := range eq {
		buf = appendString(buf, m.Name)
		buf = appendString(buf, m.Value)
	}

	return buf
}

// DecodeFetchRequest parses a request made by [EncodeFetchRequest].
func DecodeFetchRequest(data []byte) (tenant string, start, end int64, eq []fetch.EqualMatcher, err error) {
	tenant, data, err = takeString(data)
	if err != nil {
		return "", 0, 0, nil, errors.Wrap(err, "tenant")
	}

	var m int
	if start, m = binary.Varint(data); m <= 0 {
		return "", 0, 0, nil, errors.New("cluster: malformed fetch request start")
	}
	data = data[m:]

	if end, m = binary.Varint(data); m <= 0 {
		return "", 0, 0, nil, errors.New("cluster: malformed fetch request end")
	}
	data = data[m:]

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return "", 0, 0, nil, errors.New("cluster: malformed matcher count")
	}
	data = data[m:]

	eq = make([]fetch.EqualMatcher, 0, count)
	for range count {
		var name, value string
		if name, data, err = takeString(data); err != nil {
			return "", 0, 0, nil, errors.Wrap(err, "matcher name")
		}

		if value, data, err = takeString(data); err != nil {
			return "", 0, 0, nil, errors.Wrap(err, "matcher value")
		}

		eq = append(eq, fetch.EqualMatcher{Name: name, Value: value})
	}

	return tenant, start, end, eq, nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))

	return append(dst, s...)
}

func takeString(data []byte) (string, []byte, error) {
	n, m := binary.Uvarint(data)
	if m <= 0 || n > uint64(len(data)-m) {
		return "", nil, errors.New("cluster: malformed length-prefixed string")
	}

	return string(data[m : m+int(n)]), data[m+int(n):], nil
}

// EncodeBatches serializes fetch batches: each series' identity (reversible hash pre-image)
// followed by its (timestamp, value) samples. The id is recomputed from the identity on
// decode, so it is not sent.
func EncodeBatches(batches []*fetch.Batch) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(batches)))
	for _, b := range batches {
		enc := b.Series.AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)

		buf = binary.AppendUvarint(buf, uint64(len(b.Timestamps)))
		for i := range b.Timestamps {
			buf = binary.AppendVarint(buf, b.Timestamps[i])
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(b.Values[i]))
		}
	}

	return buf
}

// DecodeBatches parses [EncodeBatches] output, recomputing each batch's id from its identity.
func DecodeBatches(data []byte) ([]*fetch.Batch, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 {
		return nil, errors.New("cluster: malformed batches")
	}
	data = data[m:]

	out := make([]*fetch.Batch, 0, count)
	for range count {
		sl, m := binary.Uvarint(data)
		if m <= 0 || sl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed batch identity")
		}
		data = data[m:]

		s, _, err := signal.DecodeSeries(data[:sl])
		if err != nil {
			return nil, errors.Wrap(err, "decode series")
		}
		data = data[sl:]

		ns, m := binary.Uvarint(data)
		if m <= 0 {
			return nil, errors.New("cluster: malformed sample count")
		}
		data = data[m:]

		b := &fetch.Batch{ID: s.Hash(), Series: s}
		for range ns {
			ts, m := binary.Varint(data)
			if m <= 0 || len(data)-m < 8 {
				return nil, errors.New("cluster: malformed sample")
			}
			data = data[m:]
			b.Timestamps = append(b.Timestamps, ts)
			b.Values = append(b.Values, math.Float64frombits(binary.BigEndian.Uint64(data)))
			data = data[8:]
		}

		out = append(out, b)
	}

	return out, nil
}

// FetchFunc fetches a tenant's series within [start, end] from the local store, applying the
// pushed-down matchers. It is what [ReadHandler] serves.
type FetchFunc func(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error)

// ReadHandler returns the HTTP handler that serves fetches from the local store, reconstructing
// the pushed-down equality matchers. Mount it on the node's server at [ReadPath].
func ReadHandler(fetchFn FetchFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		tenant, start, end, eq, err := DecodeFetchRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		matchers := make([]fetch.Matcher, len(eq))
		for i := range eq {
			matchers[i] = fetch.Matcher{Name: []byte(eq[i].Name), Match: eq[i].Predicate(), Spec: &eq[i]}
		}

		batches, err := fetchFn(req.Context(), tenant, start, end, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = w.Write(EncodeBatches(batches))
	})
}

// RemoteFetcher is a [fetch.Fetcher] over a peer node's [ReadHandler]. It forwards only the
// request's tenant and window (matchers are re-applied by the caller), so it returns the
// peer's full window — a superset the fetch contract permits.
type RemoteFetcher struct {
	addr   string
	client *http.Client
}

// NewRemoteFetcher returns a fetcher that reads from the peer at addr. A nil client uses
// [http.DefaultClient].
func NewRemoteFetcher(addr string, client *http.Client) *RemoteFetcher {
	if client == nil {
		client = http.DefaultClient
	}

	return &RemoteFetcher{addr: addr, client: client}
}

// Fetch forwards r's tenant, window, and serializable (equality) matchers to the peer and
// returns the decoded batches. Non-equality matchers are not forwarded — the requester
// re-applies the full matcher set to the (possibly superset) result.
func (f *RemoteFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	var eq []fetch.EqualMatcher
	for i := range r.Matchers {
		if r.Matchers[i].Spec != nil {
			eq = append(eq, *r.Matchers[i].Spec)
		}
	}

	payload := EncodeFetchRequest(string(r.Tenant), r.Start, r.End, eq)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+f.addr+ReadPath, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch from %q", f.addr)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("cluster: %q fetch returned %d: %s", f.addr, resp.StatusCode, bytes.TrimSpace(body))
	}

	batches, err := DecodeBatches(body)
	if err != nil {
		return nil, err
	}

	return fetch.NewSliceIterator(batches), nil
}

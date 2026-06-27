package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// ReadPath is the HTTP path the cluster read (fetch fan-out) server serves.
const ReadPath = "/internal/fetch"

// The cluster read RPC carries only a tenant and a time window — not the fetch matchers, which
// are opaque Go predicates (not serializable). A peer returns every series in the window (a
// superset, which the fetch contract permits); the requesting node re-applies its matchers.

// EncodeFetchRequest frames a fetch request: the signal, tenant, window, and any serializable
// equality matchers to push down to the peer (other predicates are re-checked by the requester).
func EncodeFetchRequest(sig signal.Signal, tenant string, start, end int64, eq []fetch.EqualMatcher) []byte {
	buf := []byte{byte(sig)}
	buf = appendString(buf, tenant)
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
//
//nolint:gocritic // the wire shape is signal+tenant+window+matchers+err; a struct would obscure it
func DecodeFetchRequest(data []byte) (sig signal.Signal, tenant string, start, end int64, eq []fetch.EqualMatcher, err error) {
	if len(data) < 1 {
		return 0, "", 0, 0, nil, errors.New("cluster: empty fetch request")
	}

	sig = signal.Signal(data[0])
	data = data[1:]

	tenant, data, err = takeString(data)
	if err != nil {
		return 0, "", 0, 0, nil, errors.Wrap(err, "tenant")
	}

	var m int
	if start, m = binary.Varint(data); m <= 0 {
		return 0, "", 0, 0, nil, errors.New("cluster: malformed fetch request start")
	}
	data = data[m:]

	if end, m = binary.Varint(data); m <= 0 {
		return 0, "", 0, 0, nil, errors.New("cluster: malformed fetch request end")
	}
	data = data[m:]

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return 0, "", 0, 0, nil, errors.New("cluster: malformed matcher count")
	}
	data = data[m:]

	eq = make([]fetch.EqualMatcher, 0, count)
	for range count {
		var name, value string
		if name, data, err = takeString(data); err != nil {
			return 0, "", 0, 0, nil, errors.Wrap(err, "matcher name")
		}

		if value, data, err = takeString(data); err != nil {
			return 0, "", 0, 0, nil, errors.Wrap(err, "matcher value")
		}

		eq = append(eq, fetch.EqualMatcher{Name: name, Value: value})
	}

	return sig, tenant, start, end, eq, nil
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

// Log-batch column kind tags on the wire.
const (
	colKindInt64 byte = 0
	colKindFloat byte = 1
	colKindBytes byte = 2
)

// EncodeLogBatches serializes log fetch batches: each stream's identity, its record timestamps,
// and its named per-record columns (each tagged by physical kind). The id is recomputed from the
// identity on decode, so it is not sent.
func EncodeLogBatches(batches []*fetch.Batch) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(batches)))
	for _, b := range batches {
		enc := b.Series.AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)

		buf = binary.AppendUvarint(buf, uint64(len(b.Timestamps)))
		for _, t := range b.Timestamps {
			buf = binary.AppendVarint(buf, t)
		}

		buf = binary.AppendUvarint(buf, uint64(len(b.Columns)))
		for i := range b.Columns {
			buf = appendColumn(buf, &b.Columns[i])
		}
	}

	return buf
}

func appendColumn(buf []byte, c *fetch.NamedColumn) []byte {
	buf = appendString(buf, c.Name)

	switch {
	case c.Bytes != nil:
		buf = append(buf, colKindBytes)
		buf = binary.AppendUvarint(buf, uint64(len(c.Bytes)))
		for _, v := range c.Bytes {
			buf = appendString(buf, string(v))
		}
	case c.Float64 != nil:
		buf = append(buf, colKindFloat)
		buf = binary.AppendUvarint(buf, uint64(len(c.Float64)))
		for _, v := range c.Float64 {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
		}
	default:
		buf = append(buf, colKindInt64)
		buf = binary.AppendUvarint(buf, uint64(len(c.Int64)))
		for _, v := range c.Int64 {
			buf = binary.AppendVarint(buf, v)
		}
	}

	return buf
}

// DecodeLogBatches parses [EncodeLogBatches] output, recomputing each batch's id from its identity.
func DecodeLogBatches(data []byte) ([]*fetch.Batch, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 {
		return nil, errors.New("cluster: malformed log batches")
	}

	data = data[m:]

	out := make([]*fetch.Batch, 0, count)
	for range count {
		sl, m := binary.Uvarint(data)
		if m <= 0 || sl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed log batch identity")
		}

		data = data[m:]

		s, _, err := signal.DecodeSeries(data[:sl])
		if err != nil {
			return nil, errors.Wrap(err, "decode stream")
		}

		data = data[sl:]

		b := &fetch.Batch{ID: s.Hash(), Series: s}

		if b.Timestamps, data, err = decodeTimestamps(data); err != nil {
			return nil, err
		}

		nc, m := binary.Uvarint(data)
		if m <= 0 {
			return nil, errors.New("cluster: malformed column count")
		}

		data = data[m:]

		for range nc {
			var col fetch.NamedColumn
			if col, data, err = decodeColumn(data); err != nil {
				return nil, err
			}

			b.Columns = append(b.Columns, col)
		}

		out = append(out, b)
	}

	return out, nil
}

func decodeTimestamps(data []byte) ([]int64, []byte, error) {
	n, m := binary.Uvarint(data)
	if m <= 0 {
		return nil, nil, errors.New("cluster: malformed timestamp count")
	}

	data = data[m:]
	ts := make([]int64, 0, n)

	for range n {
		t, m := binary.Varint(data)
		if m <= 0 {
			return nil, nil, errors.New("cluster: malformed timestamp")
		}

		data = data[m:]
		ts = append(ts, t)
	}

	return ts, data, nil
}

func decodeColumn(data []byte) (fetch.NamedColumn, []byte, error) {
	name, data, err := takeString(data)
	if err != nil {
		return fetch.NamedColumn{}, nil, errors.Wrap(err, "column name")
	}

	if len(data) < 1 {
		return fetch.NamedColumn{}, nil, errors.New("cluster: missing column kind")
	}

	kind := data[0]
	data = data[1:]

	n, m := binary.Uvarint(data)
	if m <= 0 {
		return fetch.NamedColumn{}, nil, errors.New("cluster: malformed column length")
	}

	data = data[m:]
	col := fetch.NamedColumn{Name: name}

	switch kind {
	case colKindBytes:
		col.Bytes = make([][]byte, 0, n)
		for range n {
			var v string
			if v, data, err = takeString(data); err != nil {
				return fetch.NamedColumn{}, nil, errors.Wrap(err, "column bytes")
			}

			col.Bytes = append(col.Bytes, []byte(v))
		}
	case colKindFloat:
		col.Float64 = make([]float64, 0, n)
		for range n {
			if len(data) < 8 {
				return fetch.NamedColumn{}, nil, errors.New("cluster: malformed float column")
			}

			col.Float64 = append(col.Float64, math.Float64frombits(binary.BigEndian.Uint64(data)))
			data = data[8:]
		}
	default:
		col.Int64 = make([]int64, 0, n)
		for range n {
			v, m := binary.Varint(data)
			if m <= 0 {
				return fetch.NamedColumn{}, nil, errors.New("cluster: malformed int column")
			}

			data = data[m:]
			col.Int64 = append(col.Int64, v)
		}
	}

	return col, data, nil
}

// FetchFunc fetches a tenant's series within [start, end] from the local store, applying the
// pushed-down matchers. It is what [ReadHandler] serves.
type FetchFunc func(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error)

// ReadHandler returns the HTTP handler that serves fetches from the local store, reconstructing
// the pushed-down equality matchers and dispatching to the metric, log, trace, or profile fetch by
// the request's signal (encoding the result with the matching batch codec — samples for metrics,
// columns for the record signals). Mount it at [ReadPath].
func ReadHandler(metricFn, logFn, traceFn, profileFn FetchFunc) http.Handler {
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

		sig, tenant, start, end, eq, err := DecodeFetchRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		matchers := make([]fetch.Matcher, len(eq))
		for i := range eq {
			matchers[i] = fetch.Matcher{Name: []byte(eq[i].Name), Match: eq[i].Predicate(), Spec: &eq[i]}
		}

		fn, encode := metricFn, EncodeBatches
		switch sig { //nolint:exhaustive // metric is the default
		case signal.Log:
			fn, encode = logFn, EncodeLogBatches
		case signal.Trace:
			fn, encode = traceFn, EncodeLogBatches // record signals share the column codec
		case signal.Profile:
			fn, encode = profileFn, EncodeLogBatches
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace (peer fetch spans nest)

		// When the caller is collecting EXPLAIN ANALYZE, run the fetch under a profile collector and
		// return the peer's subtree ahead of the batches so the requester can graft it.
		var coll *profile.Collector
		if req.Header.Get(profileHeader) == "1" {
			ctx, coll = profile.WithCollector(ctx)
		}

		batches, err := fn(ctx, tenant, start, end, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		out := encode(batches)

		// The batches are now serialized into out and no longer needed — release them so a producing
		// engine recycles their buffers (a no-op for batches without a release hook).
		for _, b := range batches {
			b.Release()
		}

		if coll != nil {
			tree := coll.Root().Encode(nil)
			framed := binary.AppendUvarint(nil, uint64(len(tree)))
			framed = append(framed, tree...)
			out = append(framed, out...)
		}

		_, _ = w.Write(out)
	})
}

// profileHeader opts a read RPC into returning the peer's EXPLAIN ANALYZE subtree (framed ahead of
// the batches): [uvarint len][profile bytes][batches]. Absent ⇒ the plain batches response.
const profileHeader = "X-Oteldb-Profile"

// RemoteFetcher is a [fetch.Fetcher] over a peer node's [ReadHandler]. It forwards only the
// request's tenant and window (matchers are re-applied by the caller), so it returns the
// peer's full window — a superset the fetch contract permits.
type RemoteFetcher struct {
	sig    signal.Signal
	addr   string
	client *http.Client
}

// NewRemoteFetcher returns a fetcher that reads the given signal from the peer at addr. A nil
// client uses [http.DefaultClient]. The zero signal value reads metrics.
func NewRemoteFetcher(sig signal.Signal, addr string, client *http.Client) *RemoteFetcher {
	if client == nil {
		client = http.DefaultClient
	}

	return &RemoteFetcher{sig: sig, addr: addr, client: client}
}

// Fetch forwards r's tenant, window, and serializable (equality) matchers to the peer and
// returns the decoded batches. Non-equality matchers (and columnar conditions) are not forwarded —
// the requester re-applies them to the (possibly superset) result.
func (f *RemoteFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	var eq []fetch.EqualMatcher
	for i := range r.Matchers {
		if r.Matchers[i].Spec != nil {
			eq = append(eq, *r.Matchers[i].Spec)
		}
	}

	payload := EncodeFetchRequest(f.sig, string(r.Tenant), r.Start, r.End, eq)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+f.addr+ReadPath, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header) // carry the trace into the read fan-out

	wantProfile := profile.Active(ctx)
	if wantProfile {
		req.Header.Set(profileHeader, "1")
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

	if wantProfile {
		body, err = f.graftProfile(ctx, body)
		if err != nil {
			return nil, err
		}
	}

	decode := DecodeBatches
	if f.sig != signal.Metric { // log and trace share the column codec
		decode = DecodeLogBatches
	}

	batches, err := decode(body)
	if err != nil {
		return nil, err
	}

	return fetch.NewSliceIterator(batches), nil
}

// graftProfile strips the [uvarint len][profile] frame the peer prepended (see [profileHeader]),
// grafts the peer's subtree (labeled by the peer address) under the current profile node in ctx, and
// returns the remaining batches bytes. A malformed frame is fatal (the batch offset is unknown); a
// merely-corrupt subtree is skipped (best-effort profiling).
func (f *RemoteFetcher) graftProfile(ctx context.Context, body []byte) ([]byte, error) {
	plen, m := binary.Uvarint(body)
	if m <= 0 || plen > uint64(len(body)-m) {
		return nil, errors.New("cluster: malformed profile frame")
	}

	tree, rest := body[m:m+int(plen)], body[m+int(plen):]

	if node, _, err := profile.Decode(tree); err == nil && node != nil {
		node.Name = "remote " + f.addr
		profile.Graft(ctx, node)
	}

	return rest, nil
}

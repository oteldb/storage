package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net/http"
	"net/url"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// ReadPath is the HTTP path the cluster read (fetch fan-out) server serves.
const (
	ReadPath   = "/internal/fetch"
	httpScheme = "http"
)

// The cluster read RPC carries a tenant, a time window, and the serializable *hints* of the
// request's predicate — an equality matcher's spec, an equality condition's column and value. The
// predicates themselves are opaque Go closures, so a peer answers with a superset (which the fetch
// contract permits) and the requesting node re-applies them.

// EncodeFetchRequest frames a fetch request: the signal, tenant, window, and any serializable
// equality matchers to push down to the peer (other predicates are re-checked by the requester).
// It carries no condition hints; [FetchRequest.Encode] does.
func EncodeFetchRequest(sig signal.Signal, tenant string, start, end int64, eq []fetch.EqualMatcher) []byte {
	return FetchRequest{Signal: sig, Tenant: tenant, Start: start, End: end, Equal: eq}.Encode()
}

// DecodeFetchRequest parses a request made by [EncodeFetchRequest], dropping any condition hints.
// [ParseFetchRequest] keeps them.
//
//nolint:gocritic // the wire shape is signal+tenant+window+matchers+err; a struct would obscure it
func DecodeFetchRequest(data []byte) (sig signal.Signal, tenant string, start, end int64, eq []fetch.EqualMatcher, err error) {
	r, err := ParseFetchRequest(data)
	if err != nil {
		return 0, "", 0, 0, nil, err
	}

	return r.Signal, r.Tenant, r.Start, r.End, r.Equal, nil
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
//
// Lossy-sampling weights follow as an append-only trailer, written only when some batch carries
// them; see [appendScaleFactors] for the shape and its mixed-version behavior.
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

	return appendScaleFactors(buf, batches)
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

	if err := decodeScaleFactors(data, out); err != nil {
		return nil, err
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
// and its named per-record columns (each tagged by its [fetch.ColumnKind]). The id is recomputed
// from the identity on decode, so it is not sent. A column carrying no kind is an error rather
// than an untagged int64 column: an empty column of any kind would otherwise arrive as an empty
// int64 one.
func EncodeLogBatches(batches []*fetch.Batch) ([]byte, error) {
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
			var err error
			if buf, err = appendColumn(buf, &b.Columns[i]); err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
}

func appendColumn(buf []byte, c *fetch.NamedColumn) ([]byte, error) {
	buf = appendString(buf, c.Name)

	switch c.Kind {
	case fetch.KindBytes:
		buf = append(buf, colKindBytes)
		buf = binary.AppendUvarint(buf, uint64(len(c.Bytes)))

		for _, v := range c.Bytes {
			buf = appendString(buf, string(v))
		}
	case fetch.KindFloat64:
		buf = append(buf, colKindFloat)
		buf = binary.AppendUvarint(buf, uint64(len(c.Float64)))

		for _, v := range c.Float64 {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(v))
		}
	case fetch.KindInt64:
		buf = append(buf, colKindInt64)
		buf = binary.AppendUvarint(buf, uint64(len(c.Int64)))

		for _, v := range c.Int64 {
			buf = binary.AppendVarint(buf, v)
		}
	case fetch.KindUnknown:
		return nil, errors.Errorf("cluster: column %q has no kind", c.Name)
	default:
		return nil, errors.Errorf("cluster: column %q has unsupported kind %d", c.Name, c.Kind)
	}

	return buf, nil
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
		col.Kind = fetch.KindBytes
		col.Bytes = make([][]byte, 0, n)

		for range n {
			var v string
			if v, data, err = takeString(data); err != nil {
				return fetch.NamedColumn{}, nil, errors.Wrap(err, "column bytes")
			}

			col.Bytes = append(col.Bytes, []byte(v))
		}
	case colKindFloat:
		col.Kind = fetch.KindFloat64
		col.Float64 = make([]float64, 0, n)

		for range n {
			if len(data) < 8 {
				return fetch.NamedColumn{}, nil, errors.New("cluster: malformed float column")
			}

			col.Float64 = append(col.Float64, math.Float64frombits(binary.BigEndian.Uint64(data)))
			data = data[8:]
		}
	case colKindInt64:
		col.Kind = fetch.KindInt64
		col.Int64 = make([]int64, 0, n)

		for range n {
			v, m := binary.Varint(data)
			if m <= 0 {
				return fetch.NamedColumn{}, nil, errors.New("cluster: malformed int column")
			}

			data = data[m:]
			col.Int64 = append(col.Int64, v)
		}
	default:
		return fetch.NamedColumn{}, nil, errors.Errorf("cluster: column %q has unknown kind tag %d", name, kind)
	}

	return col, data, nil
}

// FetchFunc fetches a tenant's series within [start, end] from the local store, applying the
// pushed-down matchers. It is what [ReadHandler] serves.
type FetchFunc func(ctx context.Context, tenant string, start, end int64, matchers []fetch.Matcher) ([]*fetch.Batch, error)

// RequestFetchFunc serves one decoded read RPC from the local store. Unlike [FetchFunc] it sees
// the whole [fetch.Request] — its signal, and the columnar conditions the requester pushed down —
// so a peer prunes by them instead of scanning its whole window.
type RequestFetchFunc func(ctx context.Context, r fetch.Request) ([]*fetch.Batch, error)

// ReadHandler returns the HTTP handler that serves fetches from the local store, reconstructing
// the pushed-down equality matchers and dispatching to the metric, log, trace, or profile fetch by
// the request's signal. Column conditions cannot reach a [FetchFunc], so a peer served this way
// answers with its whole window for a condition-only request (trace-by-id); [NewReadHandler]
// pushes them down. Mount it at [ReadPath].
func ReadHandler(metricFn, logFn, traceFn, profileFn FetchFunc, opts ...Option) http.Handler {
	return NewReadHandler(func(ctx context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		fn := metricFn

		switch r.Signal { //nolint:exhaustive // metric is the default
		case signal.Log:
			fn = logFn
		case signal.Trace:
			fn = traceFn
		case signal.Profile:
			fn = profileFn
		}

		return fn(ctx, string(r.Tenant), r.Start, r.End, r.Matchers)
	}, opts...)
}

// NewReadHandler returns the HTTP handler that serves fetches from the local store, reconstructing
// the request's pushed-down matchers and column conditions and encoding the result with the codec
// matching its signal (samples for metrics, columns for the record signals). Mount it at [ReadPath].
func NewReadHandler(fetchFn RequestFetchFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

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

		decoded, err := ParseFetchRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		sig := decoded.Signal
		fetchReq := decoded.Request()

		encode := func(b []*fetch.Batch) ([]byte, error) { return EncodeBatches(b), nil }
		if sig != signal.Metric { // log, trace and profile share the column codec
			encode = EncodeLogBatches
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace (peer fetch spans nest)
		// Serve under the caller's remaining allowance, so this node stops rather than materializing a
		// response the caller has already run out of room for.
		ctx = recvBudget(ctx, req.Header)
		ctx, span := o.Tracer.Start(ctx, "cluster.serve.fetch",
			trace.WithAttributes(attribute.String("storage.signal", sig.String())))
		defer func() { endSpan(span, err) }()

		// When the caller is collecting EXPLAIN ANALYZE, run the fetch under a profile collector and
		// return the peer's subtree ahead of the batches so the requester can graft it.
		var coll *profile.Collector
		if req.Header.Get(profileHeader) == "1" {
			ctx, coll = profile.WithCollector(ctx)
		}

		var batches []*fetch.Batch
		batches, err = fetchFn(ctx, fetchReq)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(batches)))

		var out []byte
		out, err = encode(batches)

		// The batches are now serialized into out and no longer needed — release them so a producing
		// engine recycles their buffers (a no-op for batches without a release hook).
		for _, b := range batches {
			b.Release()
		}

		if err != nil {
			writeRPCError(w, err)

			return
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

// RemoteFetcher is a [fetch.Fetcher] over a peer node's read handler. It forwards the request's
// tenant, window and serializable predicates (matchers are re-applied by the caller), so it
// returns a superset the fetch contract permits.
type RemoteFetcher struct {
	sig    signal.Signal
	addr   string
	client *http.Client
	obs    *obs.Obs
}

// NewRemoteFetcher returns a fetcher that reads the given signal from the peer at addr. A nil
// client uses [http.DefaultClient]. The zero signal value reads metrics. Without [WithTracerProvider]
// its spans report through a no-op tracer.
func NewRemoteFetcher(sig signal.Signal, addr string, client *http.Client, opts ...Option) *RemoteFetcher {
	if client == nil {
		client = http.DefaultClient
	}

	return &RemoteFetcher{sig: sig, addr: addr, client: client, obs: resolveOpts(opts)}
}

// Fetch forwards r's tenant, window, and serializable (equality) predicates — both identity
// matchers and the columnar condition hints — to the peer and returns the decoded batches. The
// non-serializable predicates (a regex matcher, a condition's Match closure) stay behind, so the
// answer is a superset the requester re-applies them to.
func (f *RemoteFetcher) Fetch(ctx context.Context, r fetch.Request) (_ fetch.Iterator, err error) {
	ctx, span := f.obs.Tracer.Start(ctx, "cluster.fetch", trace.WithAttributes(
		attribute.String("storage.rpc.peer", f.addr),
		attribute.String("storage.signal", f.sig.String()),
	))
	defer func() { endSpan(span, err) }()

	var eq []fetch.EqualMatcher
	for i := range r.Matchers {
		if r.Matchers[i].Spec != nil {
			eq = append(eq, *r.Matchers[i].Spec)
		}
	}

	payload := FetchRequest{
		Signal: f.sig, Tenant: string(r.Tenant), Start: r.Start, End: r.End,
		Equal:      eq,
		Conditions: ConditionHints(r.Conditions, r.AllConditions),
	}.Encode()

	u := (&url.URL{Scheme: httpScheme, Host: f.addr}).JoinPath(ReadPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header) // carry the trace into the read fan-out
	sendBudget(ctx, req.Header)     // and what the caller still has room to accept

	wantProfile := profile.Active(ctx)
	if wantProfile {
		req.Header.Set(profileHeader, "1")
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch from %q", f.addr)
	}
	defer func() { _ = resp.Body.Close() }()

	body, releaseBody, err := readBudgetedBody(ctx, resp.Body, resp.ContentLength)
	if err != nil {
		return nil, err
	}
	// The wire bytes are transient: past the decode below the batches carry the memory and are
	// charged in their own right.
	defer releaseBody()

	if resp.StatusCode != http.StatusOK {
		return nil, statusError(f.addr, "fetch", resp.StatusCode, body)
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

	span.SetAttributes(attribute.Int("storage.rows", len(batches)))

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

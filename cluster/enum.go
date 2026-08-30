package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"slices"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// The enumeration/resolution fan-out: a non-owner node serves a record signal's series listing
// (profile types / labels) and side store (the profiles symbol store, for stack resolution) from an
// owner over HTTP. Both reuse [EncodeFetchRequest] for the request (signal + tenant + window +
// equality matchers); they differ only in the response payload. A single owner is a complete replica,
// so the caller fails over between owners rather than merging.

// SeriesPath, SidePath, and KeysPath are the HTTP paths of the series-listing, side-store, and
// attribute-key enumeration servers.
const (
	SeriesPath = "/internal/series"
	SidePath   = "/internal/side"
	KeysPath   = "/internal/keys"
)

// SeriesFunc lists the local store's stream identities for a signal+tenant matching matchers within
// the window (a zero window disables the time filter). The signal selects the engine (logs / traces
// / profiles share one enumeration RPC, dispatched by the request's signal byte).
type SeriesFunc func(
	ctx context.Context, sig signal.Signal, tenant string, start, end int64, matchers []fetch.Matcher,
) ([]signal.Series, error)

// SideFunc returns the local store's side-store tables (name → encoded payload) for a tenant.
type SideFunc func(ctx context.Context, tenant string) (map[string][]byte, error)

// KeysFunc returns the distinct record-attribute keys (with their scope bitset) present in a
// signal+tenant's records within the window.
type KeysFunc func(ctx context.Context, sig signal.Signal, tenant string, start, end int64) ([]KeyInfo, error)

// KeyInfo is one distinct attribute key and the scope(s) it was observed in, as carried over the
// keys-enumeration RPC. Scope mirrors the record engine's KeyScope bitset (resource/scope/record).
type KeyInfo struct {
	Key   []byte
	Scope uint8
}

// EncodeSeriesList serializes stream identities as length-prefixed reversible hash pre-images.
func EncodeSeriesList(series []signal.Series) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(series)))
	for i := range series {
		enc := series[i].AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	}

	return buf
}

// DecodeSeriesList parses [EncodeSeriesList] output.
func DecodeSeriesList(data []byte) ([]signal.Series, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each series needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed series list")
	}

	data = data[m:]

	out := make([]signal.Series, 0, count)
	for range count {
		sl, m := binary.Uvarint(data)
		if m <= 0 || sl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed series identity")
		}

		data = data[m:]

		s, _, err := signal.DecodeSeries(data[:sl])
		if err != nil {
			return nil, errors.Wrap(err, "decode series")
		}

		data = data[sl:]
		out = append(out, s)
	}

	return out, nil
}

// EncodeKeyList serializes a list of distinct attribute keys: a uvarint count, then per key a
// uvarint length, the key bytes, and a single scope byte.
func EncodeKeyList(keys []KeyInfo) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(keys)))
	for i := range keys {
		buf = binary.AppendUvarint(buf, uint64(len(keys[i].Key)))
		buf = append(buf, keys[i].Key...)
		buf = append(buf, keys[i].Scope)
	}

	return buf
}

// DecodeKeyList parses [EncodeKeyList] output, bounds-checking every length so a malformed or
// truncated peer response is rejected rather than panicking.
func DecodeKeyList(data []byte) ([]KeyInfo, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each key needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed key list")
	}

	data = data[m:]

	out := make([]KeyInfo, 0, count)
	for range count {
		kl, m := binary.Uvarint(data)
		if m <= 0 || kl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed key length")
		}

		data = data[m:]

		if uint64(len(data)) < kl+1 { // key bytes + the scope byte
			return nil, errors.New("cluster: truncated key entry")
		}

		key := make([]byte, kl)
		copy(key, data[:kl])
		out = append(out, KeyInfo{Key: key, Scope: data[kl]})
		data = data[kl+1:]
	}

	return out, nil
}

// EncodeSideTables serializes a side-store table set (sorted by name for determinism).
func EncodeSideTables(tables map[string][]byte) []byte {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}

	slices.Sort(names)

	buf := binary.AppendUvarint(nil, uint64(len(names)))
	for _, name := range names {
		buf = appendString(buf, name)
		buf = appendString(buf, string(tables[name]))
	}

	return buf
}

// DecodeSideTables parses [EncodeSideTables] output.
func DecodeSideTables(data []byte) (map[string][]byte, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each table needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed side tables")
	}

	data = data[m:]

	out := make(map[string][]byte, count)
	for range count {
		name, rest, err := takeString(data)
		if err != nil {
			return nil, errors.Wrap(err, "table name")
		}

		payload, rest2, err := takeString(rest)
		if err != nil {
			return nil, errors.Wrap(err, "table payload")
		}

		out[name] = []byte(payload)
		data = rest2
	}

	return out, nil
}

// SeriesHandler serves [SeriesPath]: it reconstructs the pushed-down equality matchers and lists the
// matching stream identities via fn, dispatched to the right engine by the request's signal.
func SeriesHandler(fn SeriesFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, err := decodeEnumRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx, span := o.Tracer.Start(obs.ExtractHTTP(req.Context(), req.Header), "cluster.serve.series",
			trace.WithAttributes(attribute.String("storage.signal", r.sig.String())))
		defer func() { endSpan(span, err) }()

		var series []signal.Series
		series, err = fn(ctx, r.sig, r.tenant, r.start, r.end, r.matchers)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(series)))

		_, _ = w.Write(EncodeSeriesList(series))
	})
}

// KeysHandler serves [KeysPath]: it enumerates the distinct record-attribute keys for the request's
// signal+tenant+window via fn (matchers are not used — keys are window-scoped, not matcher-scoped).
func KeysHandler(fn KeysFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, err := decodeEnumRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx, span := o.Tracer.Start(obs.ExtractHTTP(req.Context(), req.Header), "cluster.serve.keys",
			trace.WithAttributes(attribute.String("storage.signal", r.sig.String())))
		defer func() { endSpan(span, err) }()

		var keys []KeyInfo
		keys, err = fn(ctx, r.sig, r.tenant, r.start, r.end)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(keys)))

		_, _ = w.Write(EncodeKeyList(keys))
	})
}

// SideHandler serves [SidePath]: it returns the tenant's side-store tables via fn.
func SideHandler(fn SideFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, err := decodeEnumRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx, span := o.Tracer.Start(obs.ExtractHTTP(req.Context(), req.Header), "cluster.serve.side",
			trace.WithAttributes(attribute.String("storage.signal", r.sig.String())))
		defer func() { endSpan(span, err) }()

		var tables map[string][]byte
		tables, err = fn(ctx, r.tenant)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(tables)))

		_, _ = w.Write(EncodeSideTables(tables))
	})
}

// enumReq is a decoded enumeration request: the signal dispatches the handler to the right engine
// (logs/traces/profiles share one series/keys RPC), with the tenant, window, and equality matchers.
type enumReq struct {
	sig      signal.Signal
	tenant   string
	start    int64
	end      int64
	matchers []fetch.Matcher
}

// readEnumBody reads a POSTed enumeration request body, rejecting any other method.
func readEnumBody(req *http.Request) ([]byte, error) {
	if req.Method != http.MethodPost {
		return nil, errors.New("method not allowed")
	}

	return io.ReadAll(req.Body)
}

// decodeEnumRequest reads an [EncodeFetchRequest] body and reconstructs the enumeration request.
func decodeEnumRequest(req *http.Request) (enumReq, error) {
	body, err := readEnumBody(req)
	if err != nil {
		return enumReq{}, err
	}

	sig, tenant, start, end, eq, err := DecodeFetchRequest(body)
	if err != nil {
		return enumReq{}, err
	}

	matchers := make([]fetch.Matcher, len(eq))
	for i := range eq {
		matchers[i] = fetch.Matcher{Name: []byte(eq[i].Name), Match: eq[i].Predicate(), Spec: &eq[i]}
	}

	return enumReq{sig: sig, tenant: tenant, start: start, end: end, matchers: matchers}, nil
}

// FetchSeries lists a peer's stream identities for the signal+tenant+window, pushing down the
// serializable (equality) matchers; the caller re-applies any non-equality matchers. Without
// [WithTracerProvider] its spans report through a no-op tracer.
func FetchSeries(
	ctx context.Context, client *http.Client, addr string, sig signal.Signal,
	tenant string, start, end int64, eq []fetch.EqualMatcher, opts ...Option,
) (_ []signal.Series, err error) {
	ctx, span := enumClientSpan(ctx, resolveOpts(opts), "cluster.series", addr, sig)
	defer func() { endSpan(span, err) }()

	body, err := postEnum(ctx, client, addr, SeriesPath, EncodeFetchRequest(sig, tenant, start, end, eq))
	if err != nil {
		return nil, err
	}

	series, err := DecodeSeriesList(body)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("storage.rows", len(series)))

	return series, nil
}

// FetchKeys returns a peer's distinct record-attribute keys for the signal+tenant+window. Without
// [WithTracerProvider] its spans report through a no-op tracer.
func FetchKeys(
	ctx context.Context, client *http.Client, addr string, sig signal.Signal, tenant string, start, end int64,
	opts ...Option,
) (_ []KeyInfo, err error) {
	ctx, span := enumClientSpan(ctx, resolveOpts(opts), "cluster.keys", addr, sig)
	defer func() { endSpan(span, err) }()

	body, err := postEnum(ctx, client, addr, KeysPath, EncodeFetchRequest(sig, tenant, start, end, nil))
	if err != nil {
		return nil, err
	}

	keys, err := DecodeKeyList(body)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("storage.rows", len(keys)))

	return keys, nil
}

// FetchSide returns a peer's side-store tables for the signal+tenant. Without [WithTracerProvider]
// its spans report through a no-op tracer.
func FetchSide(
	ctx context.Context, client *http.Client, addr string, sig signal.Signal, tenant string, opts ...Option,
) (_ map[string][]byte, err error) {
	ctx, span := enumClientSpan(ctx, resolveOpts(opts), "cluster.side", addr, sig)
	defer func() { endSpan(span, err) }()

	body, err := postEnum(ctx, client, addr, SidePath, EncodeFetchRequest(sig, tenant, 0, 0, nil))
	if err != nil {
		return nil, err
	}

	tables, err := DecodeSideTables(body)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("storage.rows", len(tables)))

	return tables, nil
}

// enumClientSpan opens the client-side span for one enumeration/resolution RPC to a peer. A nil o
// uses a no-op observability handle, so an unconfigured caller pays nothing.
func enumClientSpan(ctx context.Context, o *obs.Obs, name, addr string, sig signal.Signal) (context.Context, trace.Span) {
	if o == nil {
		o = obs.NewNop()
	}

	//nolint:spancheck // the caller ends the returned span via endSpan.
	return o.Tracer.Start(ctx, name, trace.WithAttributes(
		attribute.String("storage.rpc.peer", addr),
		attribute.String("storage.signal", sig.String()),
	))
}

func postEnum(ctx context.Context, client *http.Client, addr, path string, payload []byte) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}

	u := (&url.URL{Scheme: httpScheme, Host: addr}).JoinPath(path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header) // carry the trace into the enumeration/resolution RPC

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "request to %q", addr)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, statusError(addr, path, resp.StatusCode, body)
	}

	return body, nil
}

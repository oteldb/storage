package cluster

import (
	"context"
	"encoding/binary"
	"net/http"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// ValuesPath is the HTTP path of the distinct-column-value enumeration server. It is the third
// member of the enumeration fan-out ([SeriesPath], [KeysPath]) and, unlike them, carries its own
// request shape: a column (or attribute key) and a result limit on top of signal+tenant+window.
const ValuesPath = "/internal/values"

// ValuesRequest selects a distinct-value enumeration on a peer: one byte column of the signal's
// schema, or — when AttrKey is set instead of Column — one key inside its per-record attributes.
// A zero Start AND End disables the time filter; a Limit ≤ 0 is unlimited.
type ValuesRequest struct {
	Signal     signal.Signal
	Tenant     string
	Column     string
	AttrKey    []byte
	Start, End int64
	Limit      int
}

// ValuesFunc enumerates the local store's distinct values for a request.
type ValuesFunc func(ctx context.Context, r ValuesRequest) ([][]byte, error)

// EncodeValuesRequest serializes a [ValuesRequest].
func EncodeValuesRequest(r ValuesRequest) []byte {
	buf := []byte{byte(r.Signal)}
	buf = appendString(buf, r.Tenant)
	buf = appendString(buf, r.Column)
	buf = appendString(buf, string(r.AttrKey))
	buf = binary.AppendVarint(buf, r.Start)
	buf = binary.AppendVarint(buf, r.End)
	buf = binary.AppendVarint(buf, int64(r.Limit))

	return buf
}

// DecodeValuesRequest parses [EncodeValuesRequest] output, bounds-checking every field so a
// malformed or truncated peer request is rejected rather than panicking.
func DecodeValuesRequest(data []byte) (ValuesRequest, error) {
	if len(data) < 1 {
		return ValuesRequest{}, errors.New("cluster: empty values request")
	}

	r := ValuesRequest{Signal: signal.Signal(data[0])}
	data = data[1:]

	var err error
	if r.Tenant, data, err = takeString(data); err != nil {
		return ValuesRequest{}, errors.Wrap(err, "tenant")
	}

	if r.Column, data, err = takeString(data); err != nil {
		return ValuesRequest{}, errors.Wrap(err, "column")
	}

	var key string
	if key, data, err = takeString(data); err != nil {
		return ValuesRequest{}, errors.Wrap(err, "attribute key")
	}

	if key != "" {
		r.AttrKey = []byte(key)
	}

	for _, f := range []*int64{&r.Start, &r.End} {
		v, m := binary.Varint(data)
		if m <= 0 {
			return ValuesRequest{}, errors.New("cluster: malformed values request window")
		}

		*f, data = v, data[m:]
	}

	limit, m := binary.Varint(data)
	if m <= 0 {
		return ValuesRequest{}, errors.New("cluster: malformed values request limit")
	}

	r.Limit = int(limit)

	return r, nil
}

// EncodeValueList serializes a list of values: a uvarint count, then per value a uvarint length and
// the bytes.
func EncodeValueList(values [][]byte) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(values)))
	for _, v := range values {
		buf = binary.AppendUvarint(buf, uint64(len(v)))
		buf = append(buf, v...)
	}

	return buf
}

// DecodeValueList parses [EncodeValueList] output, bounds-checking every length so a malformed or
// truncated peer response is rejected rather than panicking.
func DecodeValueList(data []byte) ([][]byte, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each value needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed value list")
	}

	data = data[m:]

	out := make([][]byte, 0, count)
	for range count {
		vl, m := binary.Uvarint(data)
		if m <= 0 || vl > uint64(len(data)-m) {
			return nil, errors.New("cluster: malformed value length")
		}

		data = data[m:]
		out = append(out, append([]byte(nil), data[:vl]...))
		data = data[vl:]
	}

	return out, nil
}

// ValuesHandler serves [ValuesPath]: it enumerates the request's distinct column (or attribute-key)
// values via fn.
func ValuesHandler(fn ValuesFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := readEnumBody(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		var r ValuesRequest
		if r, err = DecodeValuesRequest(body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx, span := o.Tracer.Start(obs.ExtractHTTP(req.Context(), req.Header), "cluster.serve.values",
			trace.WithAttributes(
				attribute.String("storage.signal", r.Signal.String()),
				attribute.String("storage.column", r.Column),
			))
		defer func() { endSpan(span, err) }()

		var values [][]byte
		values, err = fn(ctx, r)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(values)))

		_, _ = w.Write(EncodeValueList(values))
	})
}

// FetchValues returns a peer's distinct values for the request. Without [WithTracerProvider] its
// spans report through a no-op tracer.
func FetchValues(
	ctx context.Context, client *http.Client, addr string, r ValuesRequest, opts ...Option,
) (_ [][]byte, err error) {
	ctx, span := enumClientSpan(ctx, resolveOpts(opts), "cluster.values", addr, r.Signal)
	defer func() { endSpan(span, err) }()

	body, err := postEnum(ctx, client, addr, ValuesPath, EncodeValuesRequest(r))
	if err != nil {
		return nil, err
	}

	values, err := DecodeValueList(body)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("storage.rows", len(values)))

	return values, nil
}

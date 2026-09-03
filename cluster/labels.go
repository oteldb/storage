package cluster

import (
	"context"
	"encoding/binary"
	"net/http"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// LabelsPath is the HTTP path of the label-metadata enumeration server, the index-only twin of
// [SeriesPath]: the answer is a handful of strings, where the identity set behind it can be
// millions, so a peer resolves it from its inverted index instead of shipping every matching
// identity over the wire.
//
// Semantics match the local [fetch.LabelLister] capability, and both sides depend on them:
//
//   - The window filter is **part-granular**: a part overlapping the window contributes all of its
//     labels, so a value may be listed whose samples sit just outside. This is the documented
//     metadata boundary — Prometheus' own label endpoints are block-granular — and exact answers
//     (counts, samples) never take this path.
//   - The caller must push **only pushable matchers**. The peer returns strings, not identities, so
//     a matcher that could not be lowered into the index has nowhere to be re-checked; a caller
//     holding one must fall back to [SeriesPath] or a fetch instead.
//
// A signal whose engine has no label index answers [fetch.ErrLabelsUnsupported], which leaves the
// caller on the path it was already taking rather than surfacing an error.
const LabelsPath = "/internal/labels"

// LabelsFunc enumerates the local store's label metadata for a request: the distinct label names of
// the matching series, or — when Name is non-empty — that name's distinct values.
type LabelsFunc func(ctx context.Context, r LabelsRequest) ([]string, error)

// LabelsRequest selects a label-metadata enumeration on a peer. An empty Name asks for the label
// *names*; a non-empty one asks for that name's *values*. A zero Start AND End disables the time
// filter, as everywhere else on the enumeration seam.
type LabelsRequest struct {
	Signal     signal.Signal
	Tenant     string
	Start, End int64
	Name       []byte
	Equal      []fetch.EqualMatcher
}

// EncodeLabelsRequest serializes a [LabelsRequest]: the name discriminator, then the shared
// [EncodeFetchRequest] frame (signal + tenant + window + equality matchers). The name goes first
// because that frame ends in an append-only tail of its own.
func EncodeLabelsRequest(r LabelsRequest) []byte {
	buf := appendString(nil, string(r.Name))

	return append(buf, EncodeFetchRequest(r.Signal, r.Tenant, r.Start, r.End, r.Equal)...)
}

// DecodeLabelsRequest parses [EncodeLabelsRequest] output, bounds-checking every field so a
// malformed or truncated peer request is rejected rather than panicking.
func DecodeLabelsRequest(data []byte) (LabelsRequest, error) {
	name, rest, err := takeString(data)
	if err != nil {
		return LabelsRequest{}, errors.Wrap(err, "label name")
	}

	sig, tenant, start, end, eq, err := DecodeFetchRequest(rest)
	if err != nil {
		return LabelsRequest{}, err
	}

	r := LabelsRequest{Signal: sig, Tenant: tenant, Start: start, End: end, Equal: eq}
	if name != "" {
		r.Name = []byte(name)
	}

	return r, nil
}

// Matchers rebuilds the peer-side identity matchers from the pushed-down equalities.
func (r LabelsRequest) Matchers() []fetch.Matcher {
	out := make([]fetch.Matcher, len(r.Equal))
	for i := range r.Equal {
		eq := &r.Equal[i]
		out[i] = fetch.Matcher{Name: []byte(eq.Name), Match: eq.Predicate(), Spec: eq}
	}

	return out
}

// EncodeStringList serializes a list of strings: a uvarint count, then per string a uvarint length
// and the bytes — the same shape [EncodeValueList] writes.
func EncodeStringList(values []string) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(values)))
	for _, v := range values {
		buf = appendString(buf, v)
	}

	return buf
}

// DecodeStringList parses [EncodeStringList] output, bounds-checking every length so a malformed or
// truncated peer response is rejected rather than panicking.
func DecodeStringList(data []byte) ([]string, error) {
	count, m := binary.Uvarint(data)
	if m <= 0 || count > uint64(len(data)) { // each string needs ≥1 downstream byte
		return nil, errors.New("cluster: malformed string list")
	}

	data = data[m:]

	out := make([]string, 0, count)
	for range count {
		v, rest, err := takeString(data)
		if err != nil {
			return nil, errors.Wrap(err, "string list entry")
		}

		out, data = append(out, v), rest
	}

	return out, nil
}

// LabelsHandler serves [LabelsPath]: it enumerates the request's label names (or one name's values)
// via fn, which dispatches to the right engine by the request's signal.
func LabelsHandler(fn LabelsFunc, opts ...Option) http.Handler {
	o := resolveOpts(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := readEnumBody(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		var r LabelsRequest
		if r, err = DecodeLabelsRequest(body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx, span := o.Tracer.Start(obs.ExtractHTTP(req.Context(), req.Header), "cluster.serve.labels",
			trace.WithAttributes(
				attribute.String("storage.signal", r.Signal.String()),
				attribute.String("storage.label", string(r.Name)),
			))
		defer func() { endSpan(span, err) }()

		var values []string
		values, err = fn(ctx, r)
		if err != nil {
			writeRPCError(w, err)

			return
		}

		span.SetAttributes(attribute.Int("storage.rows", len(values)))

		_, _ = w.Write(EncodeStringList(values))
	})
}

// FetchLabels returns a peer's label names (or one name's values) for the request. A peer that
// cannot answer from an index reports [fetch.ErrLabelsUnsupported], so the caller keeps its current
// path. Without [WithTracerProvider] its spans report through a no-op tracer.
func FetchLabels(
	ctx context.Context, client *http.Client, addr string, r LabelsRequest, opts ...Option,
) (_ []string, err error) {
	ctx, span := enumClientSpan(ctx, resolveOpts(opts), "cluster.labels", addr, r.Signal)
	defer func() { endSpan(span, err) }()

	body, err := postEnum(ctx, client, addr, LabelsPath, EncodeLabelsRequest(r))
	if err != nil {
		return nil, err
	}

	values, err := DecodeStringList(body)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("storage.rows", len(values)))

	return values, nil
}

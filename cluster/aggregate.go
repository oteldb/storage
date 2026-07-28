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

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// AggregatePath is the HTTP path the cluster aggregate-pushdown server serves. A peer runs the
// step-bucketed aggregate over its local shard (using its stats sidecar where it applies) and
// returns one compact [engine.NamedAgg] per series — identity + buckets — instead of every sample,
// so a coordinator gathers and unions across shards without shipping raw points.
const AggregatePath = "/internal/aggregate"

// AggregateFunc computes a node-local step-bucketed aggregate of a tenant's metric series matching
// the (pushed-down equality) matchers, returning each series' identity so a coordinator can
// re-check the full matcher set and union across shards. It is what [AggregateHandler] serves.
type AggregateFunc func(ctx context.Context, tenant string, start, end, step int64, matchers []fetch.Matcher) ([]engine.NamedAgg, error)

// EncodeAggregateRequest frames an aggregate request: tenant, window, step, and the serializable
// equality matchers to push to the peer (the coordinator re-checks the full set on the response).
func EncodeAggregateRequest(tenant string, start, end, step int64, eq []fetch.EqualMatcher) []byte {
	buf := appendString(nil, tenant)
	buf = binary.AppendVarint(buf, start)
	buf = binary.AppendVarint(buf, end)
	buf = binary.AppendVarint(buf, step)
	buf = binary.AppendUvarint(buf, uint64(len(eq)))

	for _, m := range eq {
		buf = appendString(buf, m.Name)
		buf = appendString(buf, m.Value)
	}

	return buf
}

// DecodeAggregateRequest parses a request made by [EncodeAggregateRequest].
//
//nolint:gocritic // the wire shape is tenant+window+step+matchers+err; a struct would obscure it
func DecodeAggregateRequest(data []byte) (tenant string, start, end, step int64, eq []fetch.EqualMatcher, err error) {
	if tenant, data, err = takeString(data); err != nil {
		return "", 0, 0, 0, nil, errors.Wrap(err, "tenant")
	}

	if start, data, err = takeVarint(data, "start"); err != nil {
		return "", 0, 0, 0, nil, err
	}

	if end, data, err = takeVarint(data, "end"); err != nil {
		return "", 0, 0, 0, nil, err
	}

	if step, data, err = takeVarint(data, "step"); err != nil {
		return "", 0, 0, 0, nil, err
	}

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return "", 0, 0, 0, nil, errors.New("cluster: malformed matcher count")
	}
	data = data[m:]

	eq = make([]fetch.EqualMatcher, 0, count)
	for range count {
		var name, value string
		if name, data, err = takeString(data); err != nil {
			return "", 0, 0, 0, nil, errors.Wrap(err, "matcher name")
		}

		if value, data, err = takeString(data); err != nil {
			return "", 0, 0, 0, nil, errors.Wrap(err, "matcher value")
		}

		eq = append(eq, fetch.EqualMatcher{Name: name, Value: value})
	}

	return tenant, start, end, step, eq, nil
}

func takeVarint(data []byte, what string) (int64, []byte, error) {
	v, m := binary.Varint(data)
	if m <= 0 {
		return 0, nil, errors.Errorf("cluster: malformed %s", what)
	}

	return v, data[m:], nil
}

// EncodeAggregates serializes per-series aggregates: a count, then per series the identity (the
// reversible hash pre-image) and its step buckets.
func EncodeAggregates(aggs []engine.NamedAgg) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(aggs)))

	for i := range aggs {
		a := &aggs[i]
		enc := a.Series.AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)

		buf = binary.AppendUvarint(buf, uint64(len(a.Buckets)))
		for _, bk := range a.Buckets {
			buf = appendPointAgg(buf, bk.Start, bk.SeriesAgg)
		}
	}

	return buf
}

// DecodeAggregates parses an [EncodeAggregates] payload. It bounds-checks every length before
// slicing, so it never panics on a malformed or truncated response.
func DecodeAggregates(data []byte) ([]engine.NamedAgg, error) {
	return decodeNamedAggs(data,
		func(start int64, a engine.SeriesAgg) engine.BucketAgg {
			return engine.BucketAgg{Start: start, SeriesAgg: a}
		},
		func(s signal.Series, buckets []engine.BucketAgg) engine.NamedAgg {
			return engine.NamedAgg{Series: s, Buckets: buckets}
		})
}

// decodeNamedAggs parses the payload shape both aggregate RPCs share — a series count, then per
// series an envelope and its keyed aggregates — building each point with point and each entry with
// series. The two differ only in what a key means (bucket start vs evaluation timestamp), so the
// framing lives here once.
func decodeNamedAggs[P, T any](
	data []byte,
	point func(key int64, a engine.SeriesAgg) P,
	series func(signal.Series, []P) T,
) ([]T, error) {
	n, m := binary.Uvarint(data)
	if m <= 0 {
		return nil, errors.New("cluster: malformed aggregate count")
	}
	data = data[m:]

	out := make([]T, 0, min(n, uint64(len(data))))

	for range n {
		s, pn, rest, err := takeSeriesEnvelope(data)
		if err != nil {
			return nil, err
		}
		data = rest

		points := make([]P, 0, min(pn, uint64(len(data))))

		for range pn {
			key, agg, rest, err := takePointAgg(data)
			if err != nil {
				return nil, err
			}

			points = append(points, point(key, agg))
			data = rest
		}

		out = append(out, series(s, points))
	}

	return out, nil
}

// takeSeriesEnvelope parses the per-series header both aggregate payloads share — [uvarint len]
// [series hash pre-image][uvarint count] — returning the identity and how many aggregates follow.
func takeSeriesEnvelope(data []byte) (s signal.Series, n uint64, rest []byte, err error) {
	sl, m := binary.Uvarint(data)
	if m <= 0 || sl > uint64(len(data)-m) {
		return s, 0, nil, errors.New("cluster: malformed series length")
	}
	data = data[m:]

	s, _, err = signal.DecodeSeries(data[:sl])
	if err != nil {
		return s, 0, nil, errors.Wrap(err, "decode series")
	}
	data = data[sl:]

	n, m = binary.Uvarint(data)
	if m <= 0 {
		return s, 0, nil, errors.New("cluster: malformed aggregate count")
	}

	return s, n, data[m:], nil
}

// appendPointAgg appends one keyed aggregate — [varint key][varint count][f64 sum][f64 min][f64 max].
// The key is a bucket start ([engine.BucketAgg]) or an evaluation timestamp ([engine.WindowAgg]);
// the two aggregate shapes share this body.
func appendPointAgg(buf []byte, key int64, a engine.SeriesAgg) []byte {
	buf = binary.AppendVarint(buf, key)
	buf = binary.AppendVarint(buf, a.Count)
	buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(a.Sum))
	buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(a.Min))

	return binary.BigEndian.AppendUint64(buf, math.Float64bits(a.Max))
}

// takePointAgg parses one aggregate written by [appendPointAgg], bounds-checking before it slices.
func takePointAgg(data []byte) (key int64, a engine.SeriesAgg, rest []byte, err error) {
	key, m := binary.Varint(data)
	if m <= 0 {
		return 0, a, nil, errors.New("cluster: malformed key")
	}
	data = data[m:]

	count, m := binary.Varint(data)
	if m <= 0 {
		return 0, a, nil, errors.New("cluster: malformed count")
	}
	data = data[m:]

	if len(data) < 24 {
		return 0, a, nil, errors.New("cluster: truncated aggregate")
	}

	return key, engine.SeriesAgg{
		Count: count,
		Sum:   math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
		Min:   math.Float64frombits(binary.BigEndian.Uint64(data[8:16])),
		Max:   math.Float64frombits(binary.BigEndian.Uint64(data[16:24])),
	}, data[24:], nil
}

// AggregateHandler returns the HTTP handler that serves an aggregate from the local store. Mount it
// at [AggregatePath].
func AggregateHandler(fn AggregateFunc) http.Handler {
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

		tenant, start, end, step, eq, err := DecodeAggregateRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		matchers := make([]fetch.Matcher, len(eq))
		for i := range eq {
			matchers[i] = fetch.Matcher{Name: []byte(eq[i].Name), Match: eq[i].Predicate(), Spec: &eq[i]}
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace

		aggs, err := fn(ctx, tenant, start, end, step, matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = w.Write(EncodeAggregates(aggs))
	})
}

// RemoteAggregator runs an aggregate over a peer node's [AggregateHandler].
type RemoteAggregator struct {
	addr   string
	client *http.Client
}

// NewRemoteAggregator returns an aggregator over the peer at addr. A nil client uses
// [http.DefaultClient].
func NewRemoteAggregator(addr string, client *http.Client) *RemoteAggregator {
	if client == nil {
		client = http.DefaultClient
	}

	return &RemoteAggregator{addr: addr, client: client}
}

// Aggregate pushes the tenant, window, step, and equality matchers to the peer and returns its
// per-series aggregates.
func (a *RemoteAggregator) Aggregate(
	ctx context.Context, tenant string, start, end, step int64, eq []fetch.EqualMatcher,
) ([]engine.NamedAgg, error) {
	payload := EncodeAggregateRequest(tenant, start, end, step, eq)

	u := (&url.URL{Scheme: httpScheme, Host: a.addr}).JoinPath(AggregatePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, errors.Wrap(err, "build request")
	}

	obs.InjectHTTP(ctx, req.Header)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "aggregate from %q", a.addr)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("cluster: %q aggregate returned %d: %s", a.addr, resp.StatusCode, bytes.TrimSpace(body))
	}

	return DecodeAggregates(body)
}

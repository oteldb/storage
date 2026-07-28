package cluster

import (
	"context"
	"encoding/binary"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// AggregateWindowPath is the HTTP path the overlapping-window aggregate server serves. It is a
// separate endpoint from [AggregatePath] rather than a widened request on it: the two carry
// different results (windows keyed by their evaluation timestamp, buckets by their start), and a
// peer too old to know about windows answers 404 — an error the coordinator fails over on — instead
// of silently returning disjoint buckets for an overlapping question.
const AggregateWindowPath = "/internal/aggregate/window"

// AggregateWindowFunc computes a node-local overlapping-window aggregate of a tenant's metric series
// matching the (pushed-down equality) matchers: one aggregate per step-aligned evaluation timestamp
// over the half-open window (t-window, t]. It is what [AggregateWindowHandler] serves.
type AggregateWindowFunc func(
	ctx context.Context, tenant string, start, end int64, spec engine.WindowSpec, matchers []fetch.Matcher,
) ([]engine.NamedWindowAgg, error)

// EncodeAggregateWindowRequest frames a window-aggregate request: tenant, range, the evaluation
// grid (step, window width, anchor), and the serializable equality matchers to push to the peer
// (the coordinator re-checks the full set on the response).
func EncodeAggregateWindowRequest(tenant string, start, end int64, spec engine.WindowSpec, eq []fetch.EqualMatcher) []byte {
	buf := appendString(nil, tenant)
	buf = binary.AppendVarint(buf, start)
	buf = binary.AppendVarint(buf, end)
	buf = binary.AppendVarint(buf, spec.Step)
	buf = binary.AppendVarint(buf, spec.Window)
	buf = binary.AppendVarint(buf, spec.Anchor)
	buf = binary.AppendUvarint(buf, uint64(len(eq)))

	for _, m := range eq {
		buf = appendString(buf, m.Name)
		buf = appendString(buf, m.Value)
	}

	return buf
}

// DecodeAggregateWindowRequest parses a request made by [EncodeAggregateWindowRequest].
//
//nolint:gocritic // the wire shape is tenant+range+grid+matchers+err; a struct would obscure it
func DecodeAggregateWindowRequest(data []byte) (
	tenant string, start, end int64, spec engine.WindowSpec, eq []fetch.EqualMatcher, err error,
) {
	if tenant, data, err = takeString(data); err != nil {
		return "", 0, 0, spec, nil, errors.Wrap(err, "tenant")
	}

	for _, f := range []struct {
		dst  *int64
		what string
	}{{&start, "start"}, {&end, "end"}, {&spec.Step, "step"}, {&spec.Window, "window"}, {&spec.Anchor, "anchor"}} {
		if *f.dst, data, err = takeVarint(data, f.what); err != nil {
			return "", 0, 0, spec, nil, err
		}
	}

	count, m := binary.Uvarint(data)
	if m <= 0 {
		return "", 0, 0, spec, nil, errors.New("cluster: malformed matcher count")
	}
	data = data[m:]

	eq = make([]fetch.EqualMatcher, 0, count)

	for range count {
		var name, value string
		if name, data, err = takeString(data); err != nil {
			return "", 0, 0, spec, nil, errors.Wrap(err, "matcher name")
		}

		if value, data, err = takeString(data); err != nil {
			return "", 0, 0, spec, nil, errors.Wrap(err, "matcher value")
		}

		eq = append(eq, fetch.EqualMatcher{Name: name, Value: value})
	}

	return tenant, start, end, spec, eq, nil
}

// EncodeWindowAggregates serializes per-series window aggregates: a count, then per series the
// identity (the reversible hash pre-image) and its windows, each keyed by its evaluation timestamp.
func EncodeWindowAggregates(aggs []engine.NamedWindowAgg) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(aggs)))

	for i := range aggs {
		a := &aggs[i]
		enc := a.Series.AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)

		buf = binary.AppendUvarint(buf, uint64(len(a.Windows)))
		for _, w := range a.Windows {
			buf = appendPointAgg(buf, w.End, w.SeriesAgg)
		}
	}

	return buf
}

// DecodeWindowAggregates parses an [EncodeWindowAggregates] payload. It bounds-checks every length
// before slicing, so it never panics on a malformed or truncated response.
func DecodeWindowAggregates(data []byte) ([]engine.NamedWindowAgg, error) {
	return decodeNamedAggs(data,
		func(end int64, a engine.SeriesAgg) engine.WindowAgg {
			return engine.WindowAgg{End: end, SeriesAgg: a}
		},
		func(s signal.Series, windows []engine.WindowAgg) engine.NamedWindowAgg {
			return engine.NamedWindowAgg{Series: s, Windows: windows}
		})
}

// AggregateWindowHandler returns the HTTP handler that serves an overlapping-window aggregate from
// the local store. Mount it at [AggregateWindowPath].
// the shared halves (body read, matcher rebuild) are already factored out.
//
//nolint:dupl // same shape as the sibling handler, but the decoded request types differ;
func AggregateWindowHandler(fn AggregateWindowFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, ok := aggregateRequestBody(w, req)
		if !ok {
			return
		}

		tenant, start, end, spec, eq, err := DecodeAggregateWindowRequest(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx := obs.ExtractHTTP(req.Context(), req.Header) // join the caller's trace

		aggs, err := fn(ctx, tenant, start, end, spec, matchersFromEq(eq))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		_, _ = w.Write(EncodeWindowAggregates(aggs))
	})
}

// AggregateWindow pushes the tenant, range, evaluation grid and equality matchers to the peer and
// returns its per-series evaluation windows.
func (a *RemoteAggregator) AggregateWindow(
	ctx context.Context, tenant string, start, end int64, spec engine.WindowSpec, eq []fetch.EqualMatcher,
) ([]engine.NamedWindowAgg, error) {
	payload := EncodeAggregateWindowRequest(tenant, start, end, spec, eq)

	body, err := a.post(ctx, AggregateWindowPath, payload, "window aggregate")
	if err != nil {
		return nil, err
	}

	return DecodeWindowAggregates(body)
}

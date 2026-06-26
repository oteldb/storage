package promql

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	promql "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/oteldb/storage/query"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// DefaultLookbackDelta is the staleness window an instant vector selector looks back over
// for the most recent sample (Prometheus' default).
const DefaultLookbackDelta = 5 * time.Minute

// Params are the evaluation parameters for a PromQL query. Times are unix nanoseconds (the
// storage time unit). Step == 0 means an instant query at End; Step > 0 a range query over
// [Start, End] at that step.
type Params struct {
	Text       string
	Start, End int64
	Step       int64
}

// Engine evaluates PromQL queries over a tenant's fetcher using the upstream Prometheus
// promql.Engine. It is safe for concurrent use.
type Engine struct {
	engine *promql.Engine
}

// NewEngine builds a PromQL engine with Prometheus-default semantics.
func NewEngine() *Engine {
	return &Engine{engine: promql.NewEngine(promql.EngineOpts{
		MaxSamples:           50_000_000,
		Timeout:              2 * time.Minute,
		LookbackDelta:        DefaultLookbackDelta,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})}
}

// Eval runs p against fetcher for one tenant and returns the result in the neutral
// [query.Result] shape.
func (e *Engine) Eval(ctx context.Context, fetcher fetch.Fetcher, tenant signal.TenantID, p Params) (query.Result, error) {
	q := NewQueryable(fetcher, tenant)

	pq, err := e.newQuery(ctx, q, p)
	if err != nil {
		return query.Result{}, err
	}
	defer pq.Close()

	res := pq.Exec(ctx)
	if res.Err != nil {
		return query.Result{}, errors.Wrap(res.Err, "exec")
	}

	return convertValue(res.Value)
}

func (e *Engine) newQuery(ctx context.Context, q *Queryable, p Params) (promql.Query, error) {
	if p.Step <= 0 {
		pq, err := e.engine.NewInstantQuery(ctx, q, nil, p.Text, time.Unix(0, p.End))
		if err != nil {
			return nil, errors.Wrap(err, "instant query")
		}

		return pq, nil
	}

	pq, err := e.engine.NewRangeQuery(ctx, q, nil, p.Text,
		time.Unix(0, p.Start), time.Unix(0, p.End), time.Duration(p.Step))
	if err != nil {
		return nil, errors.Wrap(err, "range query")
	}

	return pq, nil
}

// convertValue maps a Prometheus result value to the neutral [query.Result], converting the
// millisecond timeline back to nanoseconds.
func convertValue(v parser.Value) (query.Result, error) {
	switch val := v.(type) {
	case promql.Matrix:
		out := query.Result{Type: query.ResultMatrix, Series: make([]query.Series, len(val))}
		for i, s := range val {
			out.Series[i] = query.Series{Metric: convertLabels(s.Metric), Points: convertFloats(s.Floats)}
		}

		return out, nil
	case promql.Vector:
		out := query.Result{Type: query.ResultVector, Series: make([]query.Series, len(val))}
		for i, s := range val {
			out.Series[i] = query.Series{
				Metric: convertLabels(s.Metric),
				Points: []query.Point{{T: s.T * nsPerMs, V: s.F}},
			}
		}

		return out, nil
	case promql.Scalar:
		return query.Result{Type: query.ResultScalar, Scalar: query.Point{T: val.T * nsPerMs, V: val.V}}, nil
	case promql.String:
		return query.Result{Type: query.ResultString, String: val.V}, nil
	default:
		return query.Result{}, errors.Errorf("unsupported result value type %T", v)
	}
}

func convertFloats(pts []promql.FPoint) []query.Point {
	out := make([]query.Point, len(pts))
	for i, p := range pts {
		out[i] = query.Point{T: p.T * nsPerMs, V: p.F}
	}

	return out
}

package engine_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// metricStore instantiates the shared merge conformance suite over the metric engine.
type metricStore struct {
	e      *engine.Engine
	stream map[signal.SeriesID]int
}

func (s *metricStore) Ingest(ctx context.Context, rows []mergestreamtest.Row) error {
	var (
		ids    = make([]signal.SeriesID, len(rows))
		tss    = make([]int64, len(rows))
		vals   = make([]float64, len(rows))
		series = make([]signal.Series, len(rows))
	)

	for i, r := range rows {
		series[i] = s.series(r.Stream)
		ids[i], tss[i], vals[i] = series[i].Hash(), r.Ts, float64(r.Val)
	}

	if _, err := s.e.AppendBatch(ids, tss, vals, nil,
		func(i int) signal.Series { return series[i] }, engine.AppendLimits{}); err != nil {
		return err
	}

	return s.e.Flush(ctx)
}

func (s *metricStore) Merge(ctx context.Context) error { return s.e.Merge(ctx, 0) }

func (s *metricStore) Rows(ctx context.Context) ([]mergestreamtest.Row, error) {
	it, err := s.e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	if err != nil {
		return nil, err
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	var out []mergestreamtest.Row

	for _, b := range batches {
		for i, ts := range b.Timestamps {
			out = append(out, mergestreamtest.Row{Stream: s.stream[b.ID], Ts: ts, Val: int64(b.Values[i])})
		}
	}

	return out, nil
}

func (s *metricStore) Parts() []string { return s.e.PartPrefixes() }

// Want dedups by (series, ts) with the later source part winning, which is what the metric merge
// does and the record merge does not.
func (s *metricStore) Want(src [][]mergestreamtest.Row) []mergestreamtest.Row {
	type key struct {
		stream int
		ts     int64
	}

	last := map[key]int64{}
	for _, rows := range src {
		for _, r := range rows {
			last[key{r.Stream, r.Ts}] = r.Val
		}
	}

	out := make([]mergestreamtest.Row, 0, len(last))
	for k, v := range last {
		out = append(out, mergestreamtest.Row{Stream: k.stream, Ts: k.ts, Val: v})
	}

	return out
}

func (s *metricStore) series(stream int) signal.Series {
	series := mkSeries("id", strconv.Itoa(stream))
	s.stream[series.Hash()] = stream

	return series
}

func TestMergeStreamConformance(t *testing.T) {
	t.Parallel()

	mergestreamtest.Run(t, func(t *testing.T, be *mergestreamtest.Backend) mergestreamtest.Store {
		t.Helper()

		return &metricStore{
			e:      engine.New(engine.Config{Backend: be, Prefix: "default/metrics"}),
			stream: map[signal.SeriesID]int{},
		}
	})
}

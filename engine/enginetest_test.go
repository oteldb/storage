package engine_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/enginetest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// metricEngine adapts the metric engine to the shared suite: a Row is one sample of the series
// {job=<stream>}. Read also enforces the fetch contract of one batch per series.
type metricEngine struct{ *engine.Engine }

func (e metricEngine) Append(t *testing.T, rows ...enginetest.Row) {
	t.Helper()

	for _, r := range rows {
		pairs := []string{"job", r.Stream}
		if r.Attr[0] != "" {
			pairs = append(pairs, r.Attr[0], r.Attr[1])
		}

		mustAppend(t, e.Engine, mkSeries(pairs...), r.Ts, float64(r.Val))
	}
}

func (e metricEngine) Read(ctx context.Context, stream string) ([]enginetest.Row, error) {
	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", stream)}})
	if err != nil {
		return nil, err
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	var out []enginetest.Row

	seen := make(map[signal.SeriesID]struct{}, len(batches))

	for _, b := range batches {
		if _, dup := seen[b.ID]; dup {
			return nil, errors.Errorf("series %v split across batches", b.ID)
		}

		seen[b.ID] = struct{}{}

		for i, ts := range b.Timestamps {
			out = append(out, enginetest.Row{Stream: stream, Ts: ts, Val: int64(b.Values[i])})
		}
	}

	return out, nil
}

func (e metricEngine) AttrNames(t *testing.T) []string {
	t.Helper()

	names, err := e.LabelNames(context.Background(), fetch.Request{Start: 0, End: 1 << 60})
	require.NoError(t, err)

	return names
}

func (e metricEngine) ForceMerge(ctx context.Context) error {
	return e.MergeWith(ctx, engine.MergeOptions{Force: true})
}

func (e metricEngine) StreamCount() int { return e.SeriesCount() }

func (e metricEngine) HeadRows() int { return e.HeadSampleCount() }

func (e metricEngine) Parts() []enginetest.Part {
	parts := e.Engine.Parts()

	out := make([]enginetest.Part, 0, len(parts))
	for _, p := range parts {
		out = append(out, enginetest.Part{ID: p.ID, MinTime: p.MinTime, MaxTime: p.MaxTime})
	}

	return out
}

func (e metricEngine) Stats() enginetest.Stats {
	st := e.Engine.Stats()

	return enginetest.Stats{HeadAge: st.HeadAge, WantedParts: st.WantedParts, Holes: st.Holes, LostParts: st.LostParts}
}

func (e metricEngine) MergeShape() enginetest.MergeShape {
	sh := e.Engine.MergeShape()

	return enginetest.MergeShape{Parts: sh.Parts, Bytes: sh.Bytes}
}

func (e metricEngine) RepairStats() enginetest.RepairStats {
	return enginetest.RepairStats(e.Engine.RepairStats())
}

// metricFetcher bridges the suite's PartFetcher to the engine's.
type metricFetcher struct{ enginetest.PartFetcher }

func (f metricFetcher) FetchWants(ctx context.Context, wants []bucketindex.Want) []engine.FetchResult {
	res := f.PartFetcher.FetchWants(ctx, wants)

	out := make([]engine.FetchResult, len(res))
	for i := range res {
		out[i] = engine.FetchResult(res[i])
	}

	return out
}

var metricKind = enginetest.Kind{
	Name:   "metrics",
	Prefix: "default/metrics",
	Open: func(t *testing.T, cfg enginetest.Config) enginetest.Engine {
		t.Helper()

		c := engine.Config{Backend: cfg.Backend, Prefix: "default/metrics", WAL: cfg.WAL, Obs: cfg.Obs, WriterID: cfg.WriterID}
		if cfg.Repair != nil {
			c.Repair = metricFetcher{cfg.Repair}
		}

		return metricEngine{engine.New(c)}
	},
	Identity:             func(stream string) signal.Series { return mkSeries("job", stream) },
	LegacyIdentityObject: "series.bin",
	Erodes:               func(key string) bool { return !strings.HasSuffix(key, "/manifest") },
	IdentitySeed:         20_000,
	IdentityRatio:        1000,
}

func TestEngineSuite(t *testing.T) {
	t.Parallel()

	enginetest.Run(t, metricKind)
}

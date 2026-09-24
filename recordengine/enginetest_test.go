package recordengine_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/enginetest"
	"github.com/oteldb/storage/recordengine"
)

// recordEngine adapts the record engine to the shared suite: a Row is one record of the stream
// {service.name=<stream>} whose body is the decimal Val.
type recordEngine struct{ *recordengine.Engine }

func (e recordEngine) Append(t *testing.T, rows ...enginetest.Row) {
	t.Helper()

	for _, r := range rows {
		ingest(t, e.Engine, mkBatch(r.Stream, rrec{ts: r.Ts, body: strconv.FormatInt(r.Val, 10), attr: r.Attr}))
	}
}

func (e recordEngine) Rows(t *testing.T, stream string) []enginetest.Row {
	t.Helper()

	var out []enginetest.Row

	for _, b := range fetchAll(t, e.Engine, req(stream)) {
		for i, body := range bodies(b) {
			v, err := strconv.ParseInt(body, 10, 64)
			require.NoError(t, err)

			out = append(out, enginetest.Row{Stream: stream, Ts: b.Timestamps[i], Val: v})
		}
	}

	return out
}

func (e recordEngine) AttrNames(*testing.T) []string {
	keys := e.Keys(0, 1<<60)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, string(k.Key))
	}

	return out
}

func (e recordEngine) StreamCount() int { return int(e.Engine.Stats().Streams) }

func (e recordEngine) Parts() []enginetest.Part {
	parts := e.Engine.Parts()

	out := make([]enginetest.Part, 0, len(parts))
	for _, p := range parts {
		out = append(out, enginetest.Part{ID: p.ID, MinTime: p.MinTime, MaxTime: p.MaxTime})
	}

	return out
}

func (e recordEngine) Stats() enginetest.Stats {
	st := e.Engine.Stats()

	return enginetest.Stats{HeadAge: st.HeadAge, WantedParts: st.WantedParts, Holes: st.Holes, LostParts: st.LostParts}
}

func (e recordEngine) MergeShape() enginetest.MergeShape {
	sh := e.Engine.MergeShape()

	return enginetest.MergeShape{Parts: sh.Parts, Bytes: sh.Bytes}
}

func (e recordEngine) RepairStats() enginetest.RepairStats {
	return enginetest.RepairStats(e.Engine.RepairStats())
}

// recordFetcher bridges the suite's PartFetcher to the engine's.
type recordFetcher struct{ enginetest.PartFetcher }

func (f recordFetcher) FetchWants(ctx context.Context, wants []bucketindex.Want) []recordengine.FetchResult {
	res := f.PartFetcher.FetchWants(ctx, wants)

	out := make([]recordengine.FetchResult, len(res))
	for i := range res {
		out[i] = recordengine.FetchResult(res[i])
	}

	return out
}

var recordKind = enginetest.Kind{
	Name:   "records",
	Prefix: enginePrefix,
	Open: func(t *testing.T, cfg enginetest.Config) enginetest.Engine {
		t.Helper()

		c := recordengine.Config{Schema: testSchema, Backend: cfg.Backend, Prefix: enginePrefix, WAL: cfg.WAL, Obs: cfg.Obs}
		if cfg.Repair != nil {
			c.Repair = recordFetcher{cfg.Repair}
		}

		return recordEngine{recordengine.New(c)}
	},
	Identity:             streamIdentity,
	LegacyIdentityObject: "streams.bin",
	Erodes:               func(key string) bool { return strings.Contains(key, "/c/") },
	IdentitySeed:         2_000,
	IdentityRatio:        100,
}

func TestEngineSuite(t *testing.T) {
	t.Parallel()

	enginetest.Run(t, recordKind)
}

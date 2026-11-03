package engine_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

func TestMetricFloatGroupsMixedParts(t *testing.T) {
	t.Parallel()
	for _, files := range []bool{false, true} {
		for _, groupedMerge := range []bool{false, true} {
			t.Run(strconv.FormatBool(files)+"/merge-"+strconv.FormatBool(groupedMerge), func(t *testing.T) {
				t.Parallel()
				b := backend.Memory()
				if files {
					file, err := backendfile.New(t.TempDir())
					require.NoError(t, err)
					b = file
				}
				cfg := engine.Config{Backend: b, Prefix: "mixed/metrics", MetricBlockRows: 16, DecodeCacheBytes: 1 << 20}
				old := engine.New(cfg)
				series := []string{"a", "b"}
				values := func(i int) float64 {
					switch i % 10 {
					case 2:
						return math.Copysign(0, -1)
					case 3:
						return math.Float64frombits(0x7ff0000000000002)
					default:
						return float64(i)
					}
				}
				appendRows := func(e *engine.Engine, lo, hi int) {
					for _, name := range series {
						for i := lo; i < hi; i++ {
							mustAppend(t, e, mkSeries("instance", name), int64(i+1), values(i))
						}
					}
				}
				appendRows(old, 0, 60)
				require.NoError(t, old.Flush(t.Context()))
				cfg.MetricFloatGroups = true
				current := engine.New(cfg)
				require.NoError(t, current.LoadParts(t.Context()))
				appendRows(current, 60, 120)
				require.NoError(t, current.Flush(t.Context()))
				parts, err := current.PartsDetailed(t.Context())
				require.NoError(t, err)
				require.Len(t, parts, 2)
				var grouped bool
				for _, p := range parts {
					for _, c := range p.Columns {
						grouped = grouped || c.Name == "value" && c.Codec == "floatgroups"
					}
				}
				require.True(t, grouped)
				cfg.MetricFloatGroups = groupedMerge
				reopened := engine.New(cfg)
				require.NoError(t, reopened.LoadParts(t.Context()))
				check := func() {
					got := fetchAll(t, reopened, fetch.Request{Start: 0, End: 1000})
					require.Len(t, got, 2)
					for _, s := range got {
						require.Len(t, s.Values, 120)
						for i, v := range s.Values {
							require.Equal(t, math.Float64bits(values(i)), math.Float64bits(v))
						}
					}
					count, err := reopened.Count(t.Context(), fetch.Request{Start: 50, End: 55})
					require.NoError(t, err)
					require.Equal(t, 2, count)
				}
				check()
				require.NoError(t, reopened.MergeWith(t.Context(), engine.MergeOptions{Force: true}))
				check()
			})
		}
	}
}

func TestMetricFloatGroupsWALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := wal.Create(dir, 64)
	require.NoError(t, err)
	e := engine.New(engine.Config{WAL: w, MetricFloatGroups: true})
	s := mkSeries("job", "replay")
	values := []float64{1, math.Copysign(0, -1), math.Float64frombits(0x7ff0000000000002), math.Inf(1), 3}
	for i, v := range values {
		mustAppend(t, e, s, int64(i+1), v)
	}
	require.NoError(t, w.Close())
	restored := engine.New(engine.Config{MetricFloatGroups: true, Backend: backend.Memory()})
	require.NoError(t, restored.Replay(t.Context(), dir))
	require.NoError(t, restored.Flush(t.Context()))
	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 10})
	require.Len(t, got, 1)
	require.Len(t, got[0].Values, len(values))
	for i, v := range values {
		require.Equal(t, math.Float64bits(v), math.Float64bits(got[0].Values[i]))
	}
}

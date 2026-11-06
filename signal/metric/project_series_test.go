package metric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestProjectSeriesIdentityRuns(t *testing.T) {
	t.Parallel()
	a := signal.NewAttributes(kv("instance", str("host-a")))
	b := signal.NewAttributes(kv("instance", str("host-b")))
	overlap := signal.NewAttributes(kv("a", str("first")), kv("b", str("second")))
	for _, tt := range []struct {
		name string
		sets []signal.Attributes
	}{
		{"shared", []signal.Attributes{a, a, a}},
		{"cloned", []signal.Attributes{a, a.Clone(), a.Clone()}},
		{"mixed-runs", []signal.Attributes{a, a.Clone(), b, b, a}},
		{"interleaved", []signal.Attributes{a, b, a, b}},
		{"empty", []signal.Attributes{nil, {}, a, nil, {}}},
		{"overlapping-slices", []signal.Attributes{overlap[:1], overlap, overlap[:1]}},
		{"typed-and-float-bits", []signal.Attributes{
			signal.NewAttributes(kv("v", signal.IntValue(0))),
			signal.NewAttributes(kv("v", signal.DoubleValue(0))),
			signal.NewAttributes(kv("v", signal.DoubleValue(math.Copysign(0, -1)))),
			signal.NewAttributes(kv("v", signal.DoubleValue(math.Float64frombits(0x7ff8000000000001)))),
			signal.NewAttributes(kv("v", signal.DoubleValue(math.Float64frombits(0x7ff8000000000002)))),
		}},
		{"nested", []signal.Attributes{
			signal.NewAttributes(kv("v", signal.SliceValue(str("a"), signal.IntValue(1)))),
			signal.NewAttributes(kv("v", signal.SliceValue(str("a"), signal.IntValue(1)))),
			signal.NewAttributes(kv("v", signal.SliceValue(str("a"), signal.IntValue(2)))),
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var md Metrics
			mt := md.AddResource().AddScope().AddMetric()
			mt.Name, mt.Kind = []byte("counter"), KindGauge
			for i, attrs := range tt.sets {
				pt := mt.AddPoint()
				pt.Attributes, pt.Ts, pt.StartTs, pt.Value = attrs, int64(i+1), int64(i), float64(i+2)
			}
			points := mt.Points
			got, count := collect(t, md)
			require.Equal(t, len(tt.sets), count)
			for i := range got {
				require.Equal(t, points[i].Ts, got[i].s.Ts)
				require.Equal(t, points[i].StartTs, got[i].s.StartTs)
				require.Equal(t, math.Float64bits(points[i].Value), math.Float64bits(got[i].s.Value))
				require.True(t, tt.sets[i].Equal(got[i].id.Series.Attributes))
			}
		})
	}
}

func TestProjectSeriesContextAndWriteBoundaries(t *testing.T) {
	t.Parallel()
	attrs := signal.NewAttributes(kv("instance", str("host-a")))
	var md Metrics
	for resource := range 2 {
		rm := md.AddResource()
		rm.Resource.SchemaURL = []byte{byte(resource)}
		for scope := range 2 {
			sm := rm.AddScope()
			sm.Scope.Name = []byte{byte(scope)}
			for metric := range 2 {
				mt := sm.AddMetric()
				mt.Name, mt.Unit, mt.Kind = []byte{byte(metric)}, []byte{byte(metric)}, KindSum
				mt.Temporality, mt.Monotonic = Temporality(metric), metric == 1
				for range 3 {
					mt.AddPoint().Attributes = attrs
				}
			}
		}
	}
	got, _ := collect(t, md)
	unique := make(map[signal.SeriesID]struct{})
	for _, row := range got {
		unique[row.sid] = struct{}{}
	}
	require.Len(t, unique, 8)
	attrs[0].Value = str("host-b")
	updated, _ := collect(t, md)
	for i := range got {
		require.NotEqual(t, got[i].sid, updated[i].sid, "a later projection must recompute reused input")
	}
}

func FuzzProjectSeriesRuns(f *testing.F) {
	f.Add("instance", []byte{0, 0, 1, 1, 0})
	f.Add("__name__", []byte{0, 1, 0, 1})
	f.Add(".before", []byte{2, 2, 0})
	f.Fuzz(func(t *testing.T, key string, choices []byte) {
		if len(key) > 1024 || len(choices) > 128 {
			t.Skip()
		}
		sets := []signal.Attributes{nil, signal.NewAttributes(kv(key, str("first"))), signal.NewAttributes(kv(key, str("second")))}
		var md Metrics
		mt := md.AddResource().AddScope().AddMetric()
		mt.Name, mt.Kind = []byte("metric"), KindGauge
		for i, choice := range choices {
			pt := mt.AddPoint()
			pt.Attributes, pt.Ts, pt.Value = sets[int(choice)%len(sets)], int64(i), float64(i)
			if choice&4 != 0 {
				pt.Attributes = pt.Attributes.Clone()
			}
		}
		Project(md, func(batch *Batch) {
			for i := range batch.IDs {
				if batch.IDs[i] != batch.Identity(i).SeriesID() {
					t.Fatalf("identity changed at point %d", i)
				}
			}
		})
	})
}

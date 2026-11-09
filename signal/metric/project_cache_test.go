package metric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestProjectRepeatedPointsCanonical(t *testing.T) {
	t.Parallel()
	shared := signal.NewAttributes(kv("a", str("x")), kv("b", signal.IntValue(1)))
	vectors := []signal.Attributes{
		nil,
		signal.NewAttributes(kv("cpu", str("0"))),
		signal.NewAttributes(kv("__name__", str("collision"))),
		signal.NewAttributes(kv("mode", str("idle"))),
		shared[:1], shared,
	}
	for _, tc := range []struct {
		name     string
		order    []int
		accepted bool
	}{
		{"two vectors", []int{0, 1, 0, 1, 0, 1, 0, 1}, true},
		{"four vectors", []int{0, 1, 2, 3, 0, 1, 2, 3}, true},
		{"same address different lengths", []int{4, 5, 4, 5, 4, 5, 4, 5}, true},
		{"nonperiodic probe", []int{0, 1, 2, 3, 4, 5, 0, 1}, false},
		{"uniform", []int{0, 0, 0, 0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p projector
			p.setGroup(signal.Resource{}, signal.Scope{})
			metric := Metric{Name: []byte("cpu"), Kind: KindGauge}
			p.setMetric(&metric)
			points := make([]NumberPoint, len(tc.order))
			for i, index := range tc.order {
				points[i] = NumberPoint{Attributes: vectors[index], Ts: int64(i), Value: float64(i)}
			}
			var batch Batch
			require.Equal(t, tc.accepted, p.projectRepeatedPoints(&batch, points))
			if !tc.accepted {
				require.Empty(t, batch.IDs)
				require.Empty(t, batch.Ts)
				require.Empty(t, batch.Values)
				return
			}
			for i, point := range points {
				want := Identity{Series: signal.Series{Attributes: point.Attributes}, Name: metric.Name, Kind: metric.Kind}
				require.Equal(t, want.SeriesID(), batch.IDs[i])
				require.Equal(t, point.Ts, batch.Ts[i])
				require.Equal(t, math.Float64bits(point.Value), math.Float64bits(batch.Values[i]))
			}
		})
	}
}

func TestProjectPointCacheDoesNotCrossMetricOrGroup(t *testing.T) {
	t.Parallel()
	a, b := signal.NewAttributes(kv("cpu", str("0"))), signal.NewAttributes(kv("__name__", str("collision")))
	var md Metrics
	for ri := range 2 {
		rm := md.AddResource()
		rm.Resource.Attributes = signal.NewAttributes(kv("resource", signal.IntValue(int64(ri))))
		for si := range 2 {
			sm := rm.AddScope()
			sm.Scope.Name = []byte{byte('a' + si)}
			for mi := range 2 {
				mt := sm.AddMetric()
				mt.Name = []byte{byte('m' + mi)}
				mt.Unit = []byte{byte('s' + mi)}
				mt.Kind = PointKind(mi)
				mt.Temporality = Temporality(mi)
				mt.Monotonic = mi != 0
				for i := range 8 {
					point := mt.AddPoint()
					point.Ts = int64(i)
					point.Value = float64(i)
					point.Attributes = a
					if i%2 != 0 {
						point.Attributes = b
					}
				}
			}
		}
	}
	rows := 0
	accepted := Project(md, func(batch *Batch) {
		for i, id := range batch.IDs {
			require.Equal(t, batch.Series(i).Hash(), id)
			rows++
		}
	})
	require.Equal(t, 64, rows)
	require.Equal(t, rows, accepted)
}

func TestProjectPointCacheAfterPeriodicPrefix(t *testing.T) {
	t.Parallel()
	var md Metrics
	metric := md.AddResource().AddScope().AddMetric()
	metric.Name = []byte("metric")
	a, b := signal.NewAttributes(kv("cpu", str("0"))), signal.NewAttributes(kv("cpu", str("1")))
	for i := range 128 {
		point := metric.AddPoint()
		point.Ts, point.Value = int64(i), float64(i)
		point.Attributes = signal.NewAttributes(kv("unique", signal.IntValue(int64(i))))
		if i < 4 {
			point.Attributes = a
			if i%2 != 0 {
				point.Attributes = b
			}
		}
	}
	rows := 0
	accepted := Project(md, func(batch *Batch) {
		require.Len(t, batch.IDs, 128)
		for i, id := range batch.IDs {
			require.Equal(t, batch.Series(i).Hash(), id)
			require.Equal(t, int64(i), batch.Ts[i])
			require.Equal(t, math.Float64bits(float64(i)), math.Float64bits(batch.Values[i]))
			rows++
		}
	})
	require.Equal(t, 128, rows)
	require.Equal(t, rows, accepted)
}

//nolint:paralleltest // AllocsPerRun changes GOMAXPROCS.
func TestProjectPointCacheAllocations(t *testing.T) {
	var p projector
	p.setGroup(signal.Resource{}, signal.Scope{})
	p.setMetric(&Metric{Name: []byte("metric")})
	a, b := signal.NewAttributes(kv("cpu", str("0"))), signal.NewAttributes(kv("cpu", str("1")))
	points := make([]NumberPoint, 128)
	for i := range points {
		points[i].Attributes = a
		if i%2 != 0 {
			points[i].Attributes = b
		}
	}
	var batch Batch
	p.projectPoints(&batch, points)
	allocations := testing.AllocsPerRun(100, func() {
		batch.IDs = batch.IDs[:0]
		batch.Ts = batch.Ts[:0]
		batch.Values = batch.Values[:0]
		p.projectPoints(&batch, points)
	})
	require.Zero(t, allocations)
}

func FuzzProjectPointCacheCanonical(f *testing.F) {
	f.Add([]byte{0, 2, 0, 2, 0, 2, 0, 2})
	f.Add([]byte{0, 2, 3, 4, 0, 2, 3, 4})
	f.Add([]byte{0, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			return
		}
		vectors := []signal.Attributes{nil, {}, signal.NewAttributes(kv("cpu", str("0"))), signal.NewAttributes(kv("__name__", str("collision"))), signal.NewAttributes(kv("mode", str("idle")))}
		var md Metrics
		mt := md.AddResource().AddScope().AddMetric()
		mt.Name = []byte("metric")
		mt.Kind = KindSum
		mt.Monotonic = true
		mt.Temporality = TemporalityCumulative
		for i, code := range data {
			point := mt.AddPoint()
			point.Ts = int64(i)
			point.Value = math.Float64frombits(uint64(code))
			point.Attributes = vectors[int(code)%len(vectors)]
			if code&16 != 0 {
				point.Attributes = point.Attributes.Clone()
			}
			if code&32 != 0 {
				point.Attributes = signal.NewAttributes(kv("unique", signal.IntValue(int64(i))))
			}
		}
		rows := 0
		accepted := Project(md, func(batch *Batch) {
			if len(batch.IDs) != len(data) || len(batch.Ts) != len(data) || len(batch.Values) != len(data) {
				t.Fatal("projected column length changed")
			}
			for i, id := range batch.IDs {
				if id != batch.Series(i).Hash() {
					t.Fatal("cached identity differs from canonical identity")
				}
				if batch.Ts[i] != int64(i) || math.Float64bits(batch.Values[i]) != uint64(data[i]) {
					t.Fatal("sample changed")
				}
				rows++
			}
		})
		if rows != len(data) || accepted != rows {
			t.Fatal("projection dropped points")
		}
	})
}

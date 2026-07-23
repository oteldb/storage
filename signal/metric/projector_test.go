package metric

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// projectorID runs the projector over one metric/point the way [Project] does, returning the id
// it computes and whether the reserved-block fast path in [projector.id] was taken.
func projectorID(t *testing.T, id Identity, attrs signal.Attributes) (signal.SeriesID, bool) {
	t.Helper()

	var p projector
	p.setGroup(id.Series.Resource, id.Series.Scope)
	p.setMetric(&Metric{
		Name:        id.Name,
		Unit:        id.Unit,
		Kind:        id.Kind,
		Temporality: id.Temporality,
		Monotonic:   id.Monotonic,
	})

	fast := len(attrs) == 0 || bytes.Compare(p.reserved[len(p.reserved)-1].Key, attrs[0].Key) < 0

	return p.id(attrs), fast
}

func kv(k string, v signal.Value) signal.KeyValue {
	return signal.KeyValue{Key: []byte(k), Value: v}
}

func str(s string) signal.Value { return signal.StringValue([]byte(s)) }

// TestProjectorIDMatchesIdentity pins [projector.id] to the independent reference
// ([Identity.SeriesID], which sorts a combined slice) across attribute sets that take the
// reserved-block fast path and ones that fall back to the general merge.
func TestProjectorIDMatchesIdentity(t *testing.T) {
	t.Parallel()

	base := Identity{
		Series: signal.Series{
			Resource: signal.Resource{Attributes: signal.NewAttributes(kv("service.name", str("checkout")))},
			Scope:    signal.Scope{Name: []byte("lib"), Version: []byte("1.0")},
		},
		Name:        []byte("node_cpu_seconds_total"),
		Unit:        []byte("s"),
		Kind:        KindSum,
		Temporality: TemporalityCumulative,
		Monotonic:   true,
	}

	for _, tt := range []struct {
		name     string
		attrs    signal.Attributes
		wantFast bool
	}{
		{"empty", nil, true},
		{
			"typical lowercase keys",
			signal.NewAttributes(kv("cpu", str("cpu7")), kv("mode", str("idle"))),
			true,
		},
		{
			// '.' (0x2E) and digits sort before '_' (0x5F), so these keys precede the reserved
			// block and the fast path must not fire.
			"key sorting before reserved",
			signal.NewAttributes(kv(".leading.dot", str("v"))),
			false,
		},
		{
			// 'A' (0x41) < '_' (0x5F).
			"uppercase key",
			signal.NewAttributes(kv("Region", str("eu-central-1"))),
			false,
		},
		{
			// Equal to the last reserved key: the strict < keeps this on the merge path, where the
			// tie resolves to the point attribute first — matching ToSeries' stable sort.
			"key equal to last reserved",
			signal.NewAttributes(kv(string(LabelUnit), str("shadow"))),
			false,
		},
		{
			"key colliding with a middle reserved label",
			signal.NewAttributes(kv(string(LabelName), str("shadow")), kv("cpu", str("cpu0"))),
			false,
		},
		{
			"keys straddling the reserved block",
			signal.NewAttributes(kv("Aaa", str("x")), kv("zzz", str("y"))),
			false,
		},
		{
			"non-string value kinds",
			signal.NewAttributes(kv("count", signal.IntValue(7)), kv("ok", signal.BoolValue(true))),
			true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := base
			want.Series.Attributes = tt.attrs

			got, fast := projectorID(t, base, tt.attrs)
			assert.Equal(t, tt.wantFast, fast, "fast-path selection")
			require.Equal(t, want.SeriesID(), got)
		})
	}
}

// TestProjectorIDStableAcrossPoints checks that the reserved block hoisted per metric stays
// correct as the projector is reused across points with differing attribute sets — including
// alternating between the fast and fallback paths on the same metric.
func TestProjectorIDStableAcrossPoints(t *testing.T) {
	t.Parallel()

	base := Identity{Name: []byte("m"), Unit: []byte("1"), Kind: KindGauge}

	sets := []signal.Attributes{
		signal.NewAttributes(kv("cpu", str("cpu0"))),
		signal.NewAttributes(kv("Aaa", str("fallback"))),
		nil,
		signal.NewAttributes(kv("cpu", str("cpu1")), kv("mode", str("user"))),
		signal.NewAttributes(kv(string(LabelUnit), str("tie"))),
	}

	var p projector
	p.setGroup(base.Series.Resource, base.Series.Scope)
	p.setMetric(&Metric{Name: base.Name, Unit: base.Unit, Kind: base.Kind})

	for _, attrs := range sets {
		want := base
		want.Series.Attributes = attrs
		require.Equal(t, want.SeriesID(), p.id(attrs))
	}
}

// FuzzProjectorID fuzzes the fast/fallback split: any set of attribute keys must yield the same
// id as the reference implementation.
func FuzzProjectorID(f *testing.F) {
	f.Add("cpu", "mode", "node_cpu_seconds_total")
	f.Add("__unit__", "cpu", "m")
	f.Add("Aaa", "zzz", "m")
	f.Add("", "", "")
	f.Add(".dot", "__name__", "x")

	f.Fuzz(func(t *testing.T, k1, k2, name string) {
		attrs := signal.NewAttributes(kv(k1, str("v1")), kv(k2, str("v2")))
		if k1 == k2 {
			// Duplicate keys are not a valid point attribute set (the merge assumes each source is
			// individually unique); drop one rather than assert undefined behavior.
			attrs = attrs[:1]
		}

		id := Identity{Name: []byte(name), Unit: []byte("1"), Kind: KindGauge}
		got, _ := projectorID(t, id, attrs)

		want := id
		want.Series.Attributes = attrs
		if want.SeriesID() != got {
			t.Fatalf("id mismatch for keys %q,%q name %q", k1, k2, name)
		}
	})
}

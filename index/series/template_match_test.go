package series

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func templateMatchSeries(host int, value signal.Value) signal.Series {
	s := templateSeries(host, 0)
	s.Attributes = signal.NewAttributes(append(s.Attributes, signal.KeyValue{Key: []byte("value"), Value: value})...)
	return s
}

func TestPreviousTemplateTypedEquality(t *testing.T) {
	t.Parallel()
	raw := []byte{byte(signal.KindBool)}
	raw = binary.BigEndian.AppendUint64(raw, 7)
	rawBool, _, err := signal.DecodeValue(raw)
	require.NoError(t, err)
	for _, tc := range []struct {
		name         string
		first, other signal.Value
	}{
		{"empty", signal.EmptyValue(), signal.StringValue(nil)},
		{"string", sv("same"), sv("different")},
		{"bytes", signal.BytesValue([]byte("same")), sv("same")},
		{"int", signal.IntValue(1), signal.DoubleValue(1)},
		{"bool", signal.BoolValue(true), rawBool},
		{"raw-bool", rawBool, signal.BoolValue(true)},
		{"zero", signal.DoubleValue(0), signal.DoubleValue(math.Copysign(0, -1))},
		{"nan", signal.DoubleValue(math.Float64frombits(0x7ff8000000000001)), signal.DoubleValue(math.Float64frombits(0x7ff8000000000002))},
		{"slice", signal.SliceValue(signal.IntValue(1)), signal.SliceValue(sv("1"))},
		{"map", signal.MapValue(signal.KeyValue{Key: []byte("n"), Value: signal.IntValue(1)}), signal.MapValue(signal.KeyValue{Key: []byte("n"), Value: sv("1")})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewAttributeTemplates(1<<20, []byte("instance"))
			warmTemplate(p, templateMatchSeries(0, tc.first))
			ix := NewWithAttributeTemplates(nil, p)
			for i, value := range []signal.Value{tc.first, tc.first, tc.other, tc.first, tc.other} {
				s := templateMatchSeries(i, value)
				id := s.Hash()
				require.Equal(t, id, ix.AddEntry(Entry{ID: id, Series: s}))
				require.Equal(t, s.AppendHashInput(nil), ix.View().AppendCanonical(nil, i))
			}
			for _, change := range []func(*signal.Series){
				func(s *signal.Series) { s.Attributes = s.Attributes[:len(s.Attributes)-1] },
				func(s *signal.Series) { s.Attributes[len(s.Attributes)-1].Key = []byte("other") },
				func(s *signal.Series) { s.Attributes[len(s.Attributes)-1].Key = []byte("z") },
			} {
				s := templateMatchSeries(ix.Len(), tc.first)
				change(&s)
				s.Attributes = signal.NewAttributes(s.Attributes...)
				id := s.Hash()
				require.Equal(t, id, ix.AddEntry(Entry{ID: id, Series: s}))
				ref, ok := ix.Ref(id)
				require.True(t, ok)
				require.Equal(t, s.AppendHashInput(nil), ix.View().AppendCanonical(nil, int(ref)))
			}
		})
	}
}

func TestTemplateMatchBackoff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		misses int
		skip   int
	}{{"retry-immediately", 7, 0}, {"backoff", 8, 63}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var policy templateMatchPolicy
			for range tc.misses {
				require.True(t, policy.probe())
				policy.observe(false)
			}
			for range tc.skip {
				require.False(t, policy.probe())
			}
			require.True(t, policy.probe())
			policy.observe(true)
			for range 8 {
				require.True(t, policy.probe())
				policy.observe(true)
			}
			require.Zero(t, policy.misses)
		})
	}
}

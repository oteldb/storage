package fetch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMatchesSeriesAbsent(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		value   signal.Value
		present bool
	}{
		{name: "missing"},
		{name: "empty string", value: signal.StringValue(nil), present: true},
		{name: "typed value", value: signal.IntValue(7), present: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := signal.Series{}
			if tt.present {
				s.Attributes = attrs(signal.KeyValue{Key: []byte("k"), Value: tt.value})
			}
			for _, acceptAbsent := range []bool{false, true} {
				m := fetch.Matcher{Name: []byte("k"), Match: func(v signal.Value) bool {
					if v.Kind() == signal.KindEmpty {
						return acceptAbsent
					}
					return v.Equal(tt.value)
				}}
				want := tt.present || acceptAbsent
				assert.Equal(t, want, fetch.MatchesSeries(s, []fetch.Matcher{m}))
				listed := fetch.FilterSeries([]signal.Series{s}, []fetch.Matcher{m})
				assert.Equal(t, want, len(listed) == 1)
			}
		})
	}
}

package cluster

import (
	"testing"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

func FuzzAggregateStringMatchers(f *testing.F) {
	sets := []fetch.StringSetMatcher{{Name: "job", Values: fetch.AnyEqualStrings([]string{"api", "web"})}}
	f.Add(uint8(0), appendStringMatchers(EncodeAggregateRequest("t", 0, 10, 1, nil), sets))
	f.Add(uint8(1), appendStringMatchers(EncodeAggregateWindowRequest("t", 0, 10, engine.WindowSpec{Step: 1, Window: 2}, nil), sets))
	f.Fuzz(func(_ *testing.T, dimensions uint8, data []byte) {
		var eq []fetch.EqualMatcher
		var tail []byte
		var err error
		if dimensions%2 == 0 {
			_, _, _, _, eq, tail, err = decodeAggregateRequest(data)
		} else {
			_, _, _, _, eq, tail, err = decodeAggregateWindowRequest(data)
		}
		if err != nil {
			return
		}
		_, _ = hintMatchers(tail, eq)
	})
}

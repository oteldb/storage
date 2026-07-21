package recordengine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
)

func TestTSWindow(t *testing.T) {
	t.Parallel()

	// ts is ascending within a stream's part range (parts are (stream, ts)-sorted).
	ts := []int64{10, 20, 30, 40, 50}
	full := rowRange{start: 0, end: len(ts)}

	tests := []struct {
		name             string
		rng              rowRange
		start, end       int64
		wantLo, wantHigh int
	}{
		{"whole range in window", full, 0, 100, 0, 5},
		{"exact inclusive bounds", full, 20, 40, 1, 4},
		{"strict interior", full, 25, 35, 2, 3},
		{"single point on a value", full, 50, 50, 4, 5},
		{"all below window", full, 100, 200, 5, 5},
		{"all above window", full, -10, 5, 0, 0},
		{"open-ended upper (MaxInt64, no overflow)", full, 30, math.MaxInt64, 2, 5},
		{"empty input range", rowRange{start: 2, end: 2}, 0, 100, 2, 2},
		// Offset sub-range: only rows [1,4) = {20,30,40} are searched; the returned
		// bounds are absolute indices into ts.
		{"offset sub-range respects rng.start", rowRange{start: 1, end: 4}, 25, 100, 2, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tsWindow(ts, tt.rng, tt.start, tt.end)
			assert.Equal(t, tt.wantLo, got.start, "lo")
			assert.Equal(t, tt.wantHigh, got.end, "hi")
			assert.GreaterOrEqual(t, got.end, got.start, "window is well-formed")
			for i := got.start; i < got.end; i++ {
				assert.GreaterOrEqual(t, ts[i], tt.start, "row %d in window", i)
				assert.LessOrEqual(t, ts[i], tt.end, "row %d in window", i)
			}
		})
	}
}

// eqFastPathSchema has two raw fixed-width byte columns (rawA, rawB) and one dict-coded one
// (dictC), for exercising every way [eqFastPathCols] can accept or reject a condition.
var eqFastPathSchema = NewSchema(
	Column{Name: "rawA", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
	Column{Name: "rawB", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
	Column{Name: "dictC", Kind: KindBytes, Codec: chunk.CodecDict},
)

func eqCond(column, value string) fetch.Condition {
	return fetch.Condition{Column: column, Equal: &fetch.EqualMatcher{Name: column, Value: value}}
}

func TestEqFastPathCols(t *testing.T) {
	t.Parallel()

	need16 := "0123456789abcdef" // exactly 16 bytes, the EqualFixed16 kernel width

	tests := []struct {
		name  string
		conds []fetch.Condition
		want  map[int]int // byte column idx -> condition idx
	}{
		{
			name:  "qualifies",
			conds: []fetch.Condition{eqCond("rawA", need16)},
			want:  map[int]int{0: 0},
		},
		{
			name:  "wrong codec (dict) is rejected",
			conds: []fetch.Condition{eqCond("dictC", need16)},
			want:  map[int]int{},
		},
		{
			name:  "needle not 16 bytes is rejected",
			conds: []fetch.Condition{eqCond("rawA", "short")},
			want:  map[int]int{},
		},
		{
			name:  "no Equal hint is rejected",
			conds: []fetch.Condition{{Column: "rawA"}},
			want:  map[int]int{},
		},
		{
			name:  "column also projected still qualifies (rawBlob serves phase 2 too)",
			conds: []fetch.Condition{eqCond("rawA", need16)},
			want:  map[int]int{0: 0},
		},
		{
			name:  "column targeted by two conditions is rejected for both",
			conds: []fetch.Condition{eqCond("rawB", need16), eqCond("rawB", need16)},
			want:  map[int]int{},
		},
		{
			name:  "unrelated column (attrs key) is ignored, not a panic",
			conds: []fetch.Condition{eqCond("not-a-column", need16)},
			want:  map[int]int{},
		},
		{
			name:  "two independent qualifying columns both fast-path",
			conds: []fetch.Condition{eqCond("rawA", need16), eqCond("rawB", need16)},
			want:  map[int]int{0: 0, 1: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := eqFastPathCols(eqFastPathSchema, tt.conds)
			assert.Equal(t, tt.want, got)
		})
	}
}

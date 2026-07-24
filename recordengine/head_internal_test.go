package recordengine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

var headTestSchema = NewSchema(
	Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
)

func headTestRec(ts int64) rec {
	return rec{ts: ts, ints: []int64{1}, bytes: [][]byte{[]byte("body")}}
}

// TestHeadByteCap covers the byteCol format bound: a flush concatenates every stream's cells into
// one blob per byte column, indexed by int32 offsets, so the head must stop accepting records before
// its buffered bytes can overflow one — even with no MaxInFlightBytes configured. Overflowing writes
// negative offsets into a part (silent corruption), so the cap is enforced, not merely documented.
func TestHeadByteCap(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 1}

	tests := []struct {
		name    string
		bytes   int64 // pre-charged head bytes
		max     int64 // AppendLimits.MaxInFlightBytes
		outcome admitOutcome
	}{
		{"empty head, no limit", 0, 0, admitted},
		{"under both caps", 1 << 20, 1 << 30, admitted},
		{"at MaxInFlightBytes", 1 << 20, 1 << 20, rejectBytes},
		{"at the byte-column cap, no limit", math.MaxInt32, 0, rejectBytes},
		{"past the byte-column cap, no limit", math.MaxInt32 + 1, 0, rejectBytes},
		{"past the byte-column cap, higher limit", math.MaxInt32 + 1, math.MaxInt64, rejectBytes},
		{"just under the byte-column cap", math.MaxInt32 - 1, 0, admitted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHead(headTestSchema)
			h.bytes = tt.bytes

			require.Equal(t, tt.outcome, h.appendRecord(id, headTestRec(100), 0, tt.max))
		})
	}
}

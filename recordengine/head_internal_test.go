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
		name     string
		bytes    int64 // pre-charged live head bytes
		detached int64 // pre-charged in-flight-flush bytes
		max      int64 // AppendLimits.MaxInFlightBytes
		outcome  admitOutcome
	}{
		{"empty head, no limit", 0, 0, 0, admitted},
		{"under both caps", 1 << 20, 0, 1 << 30, admitted},
		{"at MaxInFlightBytes", 1 << 20, 0, 1 << 20, rejectBytes},
		{"at the byte-column cap, no limit", math.MaxInt32, 0, 0, rejectBytes},
		{"past the byte-column cap, no limit", math.MaxInt32 + 1, 0, 0, rejectBytes},
		{"past the byte-column cap, higher limit", math.MaxInt32 + 1, 0, math.MaxInt64, rejectBytes},
		{"just under the byte-column cap", math.MaxInt32 - 1, 0, 0, admitted},
		// A failed flush folds the detached buffers back into the live ones, so the two sides
		// together are what the next part's blobs are built from.
		{"detached bytes count toward the cap", 1 << 20, math.MaxInt32, 0, rejectBytes},
		{"live plus detached at the cap", math.MaxInt32 / 2, math.MaxInt32/2 + 1, 0, rejectBytes},
		{"live plus detached under the cap", 1 << 20, 1 << 20, 0, admitted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHead(headTestSchema)
			h.bytes, h.detachedBytes = tt.bytes, tt.detached

			require.Equal(t, tt.outcome, h.appendRecord(id, headTestRec(100), 0, tt.max))
		})
	}
}

// A flush that fails hands its detached buffers back to the head ([head.reattach]), so admission
// must have been counting them all along: bounding only the live half would let the reattached bytes
// land on top of a head already at the cap, and the next flush would write the negative offsets
// [headByteCap] exists to prevent.
func TestHeadByteCapSurvivesReattach(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 1}
	h := newHead(headTestSchema)

	h.bytes = headByteCap - 1
	require.Equal(t, admitted, h.appendRecord(id, headTestRec(100), 0, 0))

	detached, bytes := h.detach()
	require.NotNil(t, detached)
	require.Zero(t, h.bytes)

	require.Equal(t, rejectBytes, h.appendRecord(id, headTestRec(200), 0, 0),
		"the detached buffers are still owed back to the head")

	h.reattach(detached, bytes)
	require.LessOrEqual(t, h.bytes, int64(headByteCap)+recByteSize(headTestRec(100)),
		"reattach restores only what admission had already bounded")
}

// The out-of-order window is per stream, and a run of appends carries that stream's watermark in the
// appender rather than in the map — so a record must still be measured against the newest record of
// its own run, not only against what the head held when the run started.
func TestStreamAppenderOOOWithinRun(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 1}
	h := newHead(headTestSchema)

	a := h.appenderFor(id)
	require.Equal(t, admitted, a.append(headTestRec(1000), 100, 0), "a stream's first record is never out of order")
	require.Equal(t, admitted, a.append(headTestRec(2000), 100, 0))
	require.Equal(t, rejectOOO, a.append(headTestRec(1500), 100, 0),
		"measured against the run's own newest, not the watermark the run started with")
	require.Equal(t, admitted, a.append(headTestRec(1950), 100, 0))
	a.commit()

	require.Equal(t, int64(2000), h.streamNewest[id], "commit publishes the run's newest")
	require.Equal(t, rejectOOO, h.appendRecord(id, headTestRec(1500), 100, 0),
		"a later run sees the committed watermark")
}

// A run that admits nothing must leave the head exactly as it found it: a stream whose buffer does
// not exist stays without one, so [head.needsStreamRecord] still reports that its identity has to be
// logged again before its records are.
func TestStreamAppenderRejectedRunCreatesNothing(t *testing.T) {
	t.Parallel()

	id := signal.SeriesID{Hi: 1}
	h := newHead(headTestSchema)
	h.bytes = headByteCap

	require.True(t, h.needsStreamRecord(id))

	a := h.appenderFor(id)
	require.Equal(t, rejectBytes, a.append(headTestRec(100), 0, 0))
	a.commit()

	require.True(t, h.needsStreamRecord(id), "a fully rejected run leaves no record buffer behind")
	require.NotContains(t, h.streamNewest, id, "and no out-of-order watermark")
}

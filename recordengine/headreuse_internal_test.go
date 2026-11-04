package recordengine

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestHeadReuseBoundAndDecay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		streams, rows, body int
	}{
		{name: "many streams", streams: 2048, rows: 4, body: 64},
		{name: "large buffers", streams: 4, rows: 1024, body: 128},
		{name: "oversized buffer", streams: 1, rows: 1, body: 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema := NewSchema(Column{Name: "body", Kind: KindBytes})
			h := newHead(schema)
			r := rec{bytes: [][]byte{bytes.Repeat([]byte("x"), tc.body)}}
			for i := range tc.streams {
				for row := range tc.rows {
					r.ts = int64(row)
					require.Equal(t, admitted, h.appendRecord(signal.SeriesID{Lo: uint64(i)}, r, 0, 0))
				}
			}
			detached, _ := h.detach()
			h.releaseDetached()
			h.recycleRecordCols(detached)
			require.LessOrEqual(t, h.reusableBytes, int64(headReuseBytes))
			require.LessOrEqual(t, h.reusableCount, headReuseCount)
			var measured int64
			for c := h.reusable; c != nil; c = c.reuseNext {
				measured += c.headCapacity()
				assert.Empty(t, c.ts)
				assert.Empty(t, c.bytes[0].data)
				assert.False(t, c.keyCacheValid)
			}
			assert.Equal(t, measured, h.reusableBytes)
			r.bytes[0] = []byte("tiny")
			require.Equal(t, admitted, h.appendRecord(signal.SeriesID{Hi: 1}, r, 0, 0))
			detached, _ = h.detach()
			h.releaseDetached()
			h.recycleRecordCols(detached)
			assert.LessOrEqual(t, h.reusableBytes, int64(4096), "one small generation drops the previous high-water mark")
		})
	}
}

func TestHeadReuseAdmissionAndReattach(t *testing.T) {
	t.Parallel()
	h := newHead(headTestSchema)
	id := signal.SeriesID{Hi: 1}
	r := headTestRec(1)
	require.Equal(t, admitted, h.appendRecord(id, r, 0, 0))
	detached, size := h.detach()
	require.Zero(t, h.reusableBytes, "frozen buffers remain immutable")
	require.Equal(t, admitted, h.appendRecord(id, headTestRec(2), 0, 0))
	h.reattach(detached, size)
	assert.Equal(t, []int64{1, 2}, h.records[id].ts)
	assert.Zero(t, h.reusableBytes, "a failed flush never recycles its generation")
	detached, _ = h.detach()
	h.releaseDetached()
	h.recycleRecordCols(detached)
	require.Positive(t, h.reusableBytes)
	assert.Zero(t, h.inFlightBytes(), "idle capacity does not trigger flush pressure")
	require.Equal(t, admitted, h.appendRecord(id, headTestRec(3), 0, 1))
	assert.Zero(t, h.reusableBytes, "idle capacity is evicted before admission can reject a row")
}

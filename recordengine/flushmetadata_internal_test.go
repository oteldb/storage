package recordengine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func TestFlushMetadataSplit(t *testing.T) {
	t.Parallel()
	for _, attrs := range []bool{false, true} {
		name := "without attrs"
		if attrs {
			name = "with attrs"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mode := BloomNone
			if attrs {
				mode = BloomAttrs
			}
			schema := NewSchema(Column{Name: "attrs", Kind: KindBytes, Bloom: mode})
			records := make(map[signal.SeriesID]*recordCols)
			for i, timestamps := range [][]int64{{100, 10, 20}, {-5, 200, 0}} {
				c := newRecordCols(schema, 0, fullSel(schema))
				for j, ts := range timestamps {
					blob := signal.NewAttributes(signal.KeyValue{Key: []byte{byte('a' + 3*i + j)}, Value: signal.IntValue(ts)}).AppendHashInput(nil)
					c.appendClone(rec{ts: ts, bytes: [][]byte{blob}})
				}
				records[signal.SeriesID{Lo: uint64(i)}] = c
			}
			f := buildFlushColumns(schema, records, nil)
			for lo := range f.len() {
				for hi := lo + 1; hi <= f.len(); hi++ {
					sub := f.slice(lo, hi)
					wantMin, wantMax := maxInt64, minInt64
					for _, ts := range f.cols.ts[lo:hi] {
						wantMin, wantMax = min(wantMin, ts), max(wantMax, ts)
					}
					gotMin, gotMax := colsTimeRange(sub)
					assert.Equal(t, wantMin, gotMin)
					assert.Equal(t, wantMax, gotMax)
					nestedMin, nestedMax := colsTimeRange(sub.slice(0, sub.len()))
					assert.Equal(t, wantMin, nestedMin)
					assert.Equal(t, wantMax, nestedMax)
					be := backend.Memory()
					bb := &bloomBuilder{}
					require.NoError(t, writeBlooms(context.Background(), be, schema, "part", sub.cols, bb))
					if attrs {
						assert.True(t, sub.cols.keyCacheValid)
					}
					assert.Equal(t, distinctRecordKeys(schema, sub.cols), sub.cols.distinctAttrKeys())
					require.NoError(t, writeRecordKeys(context.Background(), be, "part", sub.cols))
					keys, err := loadRecordKeys(context.Background(), be, "part")
					require.NoError(t, err)
					assert.Equal(t, distinctRecordKeys(schema, sub.cols), keys)
				}
			}
			minTime, maxTime := colsTimeRange(f)
			assert.Equal(t, int64(-5), minTime)
			assert.Equal(t, int64(200), maxTime)
		})
	}
}

func TestFlushTimeRangeWithoutMetadata(t *testing.T) {
	t.Parallel()
	f := &flushColumns{cols: &recordCols{ts: []int64{10, -5, 100}}, stream: make([]chunk.U128, 3)}
	minTime, maxTime := colsTimeRange(f)
	assert.Equal(t, int64(-5), minTime)
	assert.Equal(t, int64(100), maxTime)
	minTime, maxTime = colsTimeRange(f.slice(1, 2))
	assert.Equal(t, int64(-5), minTime)
	assert.Equal(t, int64(-5), maxTime)
}

func TestBloomDedupHashCollisionRetainsRow(t *testing.T) {
	t.Parallel()
	var values byteCol
	values.appendCell([]byte("first"))
	values.appendCell([]byte("second"))
	bb := bloomBuilder{seen: make(map[uint64]int)}
	bb.seen[17] = 0
	assert.True(t, bb.duplicateValue(&values, 0, 17))
	assert.False(t, bb.duplicateValue(&values, 1, 17), "a collision cannot drop a different value")
	assert.False(t, bb.duplicateValue(&values, 1, 18))
}

func TestBloomSizingUsesSelectedBlob(t *testing.T) {
	t.Parallel()
	var values byteCol
	values.appendCell(make([]byte, countingWorthwhileBytes+1))
	values.appendCell([]byte("small other small"))
	part := byteCol{data: values.data, offsets: values.offsets[1:]}
	bb := &bloomBuilder{}
	bb.selectRows(BloomFullText, &part)
	assert.Equal(t, bb.countTokens(BloomFullText, &part), bb.sizeTokens(BloomFullText, &part),
		"a small split uses the occurrence shortcut even when its backing blob is large")
}

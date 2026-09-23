package recordengine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/signal"
)

// streamColumns builds flush columns holding, per service, one record per timestamp.
func streamColumns(t *testing.T, e *Engine, rows map[string][]int64) *flushColumns {
	t.Helper()

	recs := make(map[signal.SeriesID]*recordCols, len(rows))

	for svc, ts := range rows {
		series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
		)}}
		e.head.registerStream(series)

		buf := newRecordCols(headTestSchema, 0, fullSel(headTestSchema))
		for _, v := range ts {
			buf.appendClone(rec{ts: v, ints: []int64{v}, bytes: [][]byte{fmt.Appendf(nil, "%s-%d", svc, v)}})
		}

		recs[series.Hash()] = buf
	}

	return buildFlushColumns(headTestSchema, recs, nil)
}

func observeReads(t *testing.T) *[][]bool {
	t.Helper()

	var got [][]bool

	old := mergeReadObserver
	mergeReadObserver = func(streamed []bool) { got = append(got, slices.Clone(streamed)) }

	t.Cleanup(func() { mergeReadObserver = old })

	return &got
}

// TestMergeReadsOutOfOrderStreamColumnWhole: a part whose stream column is not grouped in stream
// order — which [buildRanges] tolerates by sorting its ranges — cannot be walked forward, so it is
// decoded whole while the healthy part beside it is still streamed. No row of either is lost,
// including a stream the bad part holds in two runs, and the bad part is reported once.
//
//nolint:paralleltest // observes a package-level seam
func TestMergeReadsOutOfOrderStreamColumnWhole(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order func(first, second []int) []int
	}{
		{"backwards", func(first, second []int) []int { return slices.Concat(second, first) }},
		{"repeated stream", func(first, second []int) []int {
			return slices.Concat(first[:2], second, first[2:])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			o, metrics := obstest.New(t)
			be := backend.Memory()
			e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/order", Obs: o})
			reads := observeReads(t)

			bad := streamColumns(t, e, map[string][]int64{"a": {1, 2, 3, 4}, "b": {5, 6, 7}})

			var first, second []int

			for i := range bad.len() {
				if bad.stream[i] == bad.stream[0] {
					first = append(first, i)
				} else {
					second = append(second, i)
				}
			}

			reorderRows(bad, tc.order(first, second))

			badPart := writeTestPart(t, e, be, bad)
			require.False(t, forwardReadable(badPart.ranges))

			goodPart := writeTestPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {10, 11}, "b": {12}}))

			want := mergedRows(t, []*part{badPart, goodPart})
			require.Len(t, want, 10)

			out, err := e.compactParts(ctx, []*part{badPart, goodPart}, minInt64, 0)
			require.NoError(t, err)

			assert.Equal(t, want, mergedRows(t, out))
			assert.Equal(t, [][]bool{{false, true}}, *reads)
			assert.Equal(t, int64(1),
				metrics.Counter("storage.corruption.detected", "component", "stream_order", "disposition", "tolerated"))
		})
	}
}

// writeUnblockedPart writes f the way parts were written before columns were block-framed: every
// column one stream, no marks, blooms or record keys.
func writeUnblockedPart(t *testing.T, e *Engine, be backend.Backend, f *flushColumns) *part {
	t.Helper()

	ctx := context.Background()
	w := block.NewPartWriter(block.WithSortKey(colTs))

	require.NoError(t, w.AddColumn(block.Column{Name: colStream, Kind: block.KindInt128, Int128: f.stream}))
	require.NoError(t, w.AddColumn(block.Column{
		Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: f.cols.ts,
	}))

	for k := range e.cfg.Schema.intCols {
		col := e.cfg.Schema.intColumn(k)
		require.NoError(t, w.AddColumn(block.Column{
			Name: col.Name, Kind: block.KindInt64, Codec: col.Codec, Int64: f.cols.ints[k],
		}))
	}

	for k := range e.cfg.Schema.byteCols {
		col := e.cfg.Schema.byteColumn(k)
		bc := &f.cols.bytes[k]
		require.NoError(t, w.AddColumn(block.Column{
			Name: col.Name, Kind: block.KindBytes, Codec: col.Codec, BytesBlob: bc.data, BytesOffsets: bc.offsets,
		}))
	}

	prefix := e.newPartPrefix()
	require.NoError(t, block.WritePart(ctx, be, prefix, w))

	p, err := openPart(ctx, be, e.cfg.Schema, prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)

	return p
}

// partObjects reads every object of the parts, keyed by position and name within the part.
func partObjects(t *testing.T, be backend.Backend, parts []*part) map[string][]byte {
	t.Helper()

	ctx := context.Background()
	out := make(map[string][]byte)

	for i, p := range parts {
		keys, err := be.List(ctx, p.prefix+"/")
		require.NoError(t, err)

		for _, key := range keys {
			data, err := be.Read(ctx, key)
			require.NoError(t, err)

			out[fmt.Sprintf("%d%s", i, key[len(p.prefix):])] = data
		}
	}

	return out
}

// TestMergeStreamsUnblockedPart: a part written before columns were framed has no granules to walk,
// so the cursor reads each such column whole — and the merge writes what a whole decode of the same
// sources writes.
//
//nolint:paralleltest // flips and observes package-level seams
func TestMergeStreamsUnblockedPart(t *testing.T) {
	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/legacy"})
	reads := observeReads(t)

	legacy := writeUnblockedPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {1, 2, 2, 3}, "b": {4, 5}}))

	desc, ok := legacy.reader.ColumnDescByName("body")
	require.True(t, ok)
	require.False(t, desc.Blocked)

	current := writeTestPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {2, 6}, "c": {7}}))
	src := []*part{legacy, current}

	streamed, err := e.compactParts(ctx, src, 2, 0)
	require.NoError(t, err)

	restore := SetMergeReadWhole(true)
	whole, err := e.compactParts(ctx, src, 2, 0)

	restore()
	require.NoError(t, err)

	assert.Equal(t, [][]bool{{true, true}, {false, false}}, *reads)
	assert.Equal(t, partObjects(t, be, whole), partObjects(t, be, streamed))
	assert.Len(t, mergedRows(t, streamed), 8, "retention keeps the rows at or after ts 2")
}

// TestMergeCopiesSelfGranuleEntries: a self-encoded granule's dictionary aliases the decoder's frame
// buffer, which the next frame's decompression overwrites, so the union must copy what it keeps from
// it. Two self granules in a row, each larger than a frame, are the shape that exposes an alias.
//
//nolint:paralleltest // flips package-level seams
func TestMergeCopiesSelfGranuleEntries(t *testing.T) {
	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/self", MergeCompression: compress.AlgorithmZSTD})

	const granule = 8192

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}
	e.head.registerStream(series)

	buf := newRecordCols(headTestSchema, 0, fullSel(headTestSchema))
	for i := range 3 * granule {
		body := fmt.Appendf(nil, "repeated-%d", i%5)
		if i >= granule {
			body = fmt.Appendf(nil, "unique-%08d-%x", i, i*2654435761)
		}

		buf.appendClone(rec{ts: int64(i), ints: []int64{0}, bytes: [][]byte{body}})
	}

	f := buildFlushColumns(headTestSchema, map[signal.SeriesID]*recordCols{series.Hash(): buf}, nil)
	prefix := e.newPartPrefix()
	require.NoError(t, writePart(ctx, be, headTestSchema, prefix, f, e.identitiesForColumn(f.stream),
		compress.AlgorithmZSTD, 0, e.blooms()))

	p, err := openPart(ctx, be, headTestSchema, prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)

	desc, ok := p.reader.ColumnDescByName("body")
	require.True(t, ok)
	require.True(t, desc.SharedDict, "the repeated granules must join a shared dictionary")

	streamed, err := e.compactParts(ctx, []*part{p}, minInt64, 0)
	require.NoError(t, err)

	restore := SetMergeReadWhole(true)
	whole, err := e.compactParts(ctx, []*part{p}, minInt64, 0)

	restore()
	require.NoError(t, err)

	assert.Equal(t, partObjects(t, be, whole), partObjects(t, be, streamed))
}

package recordengine

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/timebucket"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

// streamedSchema has a column of every form a merge writes: ints under two codecs, a templated body
// and attributes on a shared dictionary, a near-unique dictionary id, raw fixed and variable width
// columns, a column that is often constant, and all three bloom modes.
var streamedSchema = NewSchema(
	Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "seq", Kind: KindInt64, Codec: chunk.CodecDoD},
	Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
	Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
	Column{Name: "tid", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomEquality},
	Column{Name: "trace", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Bloom: BloomEquality},
	Column{Name: "note", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
	Column{Name: "level", Kind: KindBytes, Codec: chunk.CodecDict},
)

// streamedShape is one generated merge: its sources, where it seals, and how it writes.
type streamedShape struct {
	seed           uint64
	parts, streams int
	rows           int
	granule        int
	capBytes       int64
	retain         int64
	days           int
	zstd           bool
}

const streamedDay = int64(24 * 60 * 60 * 1e9)

// streamedEngine flushes the shape's sources: every part holds every stream, with timestamps that
// overlap across parts, repeat within them, and — when days > 1 — cross day boundaries.
func streamedEngine(t *testing.T, s streamedShape) *Engine {
	t.Helper()

	ctx := context.Background()
	cfg := Config{Schema: streamedSchema, Backend: backend.Memory(), Prefix: "t/streamed"}

	if s.zstd {
		cfg.MergeCompression = compress.AlgorithmZSTD
	}

	e := New(cfg)
	e.mergeGranule = s.granule
	r := rand.New(rand.NewPCG(s.seed, 7))

	for p := range s.parts {
		for st := range s.streams {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(st)))},
			)}}
			b := &Batch{
				Stream: series.Hash(), Identity: func() signal.Series { return series },
				Ints: make([][]int64, 2), Bytes: make([][][]byte, 6),
			}

			n := 1 + r.IntN(s.rows)
			for i := range n {
				day := int64(r.IntN(max(s.days, 1))) * streamedDay
				b.Ts = append(b.Ts, streamedDay+day+int64(p*s.rows/2+i/2)*1000+int64(r.IntN(2)))
				b.Ints[0] = append(b.Ints[0], int64(r.IntN(24)))
				b.Ints[1] = append(b.Ints[1], int64(p*s.rows+i))
				b.Bytes[0] = append(b.Bytes[0], fmt.Appendf(nil, "GET /api/v%d/orders/%d status=%d", r.IntN(3), r.IntN(40), 200+100*r.IntN(4)))

				host := "node-" + strconv.Itoa(r.IntN(6))
				if r.IntN(4) == 0 {
					host = fmt.Sprintf("req-%016x", r.Uint64())
				}

				b.Bytes[1] = append(b.Bytes[1], signal.NewAttributes(
					signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(host))},
					signal.KeyValue{Key: []byte("k" + strconv.Itoa(r.IntN(3))), Value: signal.StringValue([]byte("v"))},
				).AppendHashInput(nil))
				b.Bytes[2] = append(b.Bytes[2], fmt.Appendf(nil, "%032x", r.Uint64()%uint64(1+r.IntN(64))))

				var trace [16]byte
				for j := range trace {
					trace[j] = byte(r.Uint32())
				}

				b.Bytes[3] = append(b.Bytes[3], trace[:])
				b.Bytes[4] = append(b.Bytes[4], make([]byte, r.IntN(12)))

				level := "INFO"
				if st == 0 && r.IntN(8) == 0 {
					level = "WARN"
				}

				b.Bytes[5] = append(b.Bytes[5], []byte(level))
			}

			_, err := e.AppendBatch(b, AppendLimits{})
			require.NoError(t, err)
		}

		require.NoError(t, e.Flush(ctx))
	}

	return e
}

// rowsByStream reads every row of parts, per stream in part order then row order, as comparable
// strings.
func rowsByStream(t *testing.T, parts []*part, start int64) map[signal.SeriesID][]string {
	t.Helper()

	out := map[signal.SeriesID][]string{}
	ts := map[signal.SeriesID][]int64{}

	for _, p := range parts {
		c, err := p.readCols(context.Background(), fullSel(p.schema), nil, nil)
		require.NoError(t, err)

		for _, r := range p.ranges {
			for i := r.start; i < r.end; i++ {
				if c.ts[i] < start {
					continue
				}

				row := fmt.Appendf(nil, "%d|%d|%d", c.ts[i], c.ints[0][i], c.ints[1][i])
				for k := range c.bytes {
					row = fmt.Appendf(row, "|%x", c.bytes[k].at(i))
				}

				out[r.id] = append(out[r.id], string(row))
				ts[r.id] = append(ts[r.id], c.ts[i])
			}
		}
	}

	// A stable sort by timestamp of the rows in source order: the order the merge promises.
	for id, rows := range out {
		idx := make([]int, len(rows))
		for i := range idx {
			idx[i] = i
		}

		slices.SortStableFunc(idx, func(a, b int) int { return cmp.Compare(ts[id][a], ts[id][b]) })

		sorted := make([]string, len(rows))
		for i, j := range idx {
			sorted[i] = rows[j]
		}

		out[id] = sorted
	}

	return out
}

// checkStreamedMatchesBuffered merges the shape's sources and checks the output two ways: its rows
// are the sources' rows in merge order, and every output part matches the part [writePart] writes
// over the same rows — decoded identically, byte for byte where both writers lay a column out alike,
// and with identical blooms, record keys, watermarks and identities.
func checkStreamedMatchesBuffered(t *testing.T, s streamedShape) {
	t.Helper()

	ctx := context.Background()
	e := streamedEngine(t, s)
	src := e.parts
	want := rowsByStream(t, src, s.retain)

	out, err := e.compactParts(ctx, src, s.retain, s.capBytes)
	require.NoError(t, err)

	// Output parts of one day are sealed in order, and days do not share a timestamp, so a stable
	// sort by timestamp of the parts' rows in output order is the merge's own order.
	require.Equal(t, want, rowsByStream(t, out, minInt64))

	for _, p := range out {
		require.True(t, forwardReadable(p.ranges))

		d, err := p.readForMerge(ctx)
		require.NoError(t, err)
		require.True(t, d.tsSorted, "every stream of an output part is timestamp-ordered")

		day := timebucket.Of(p.minTime, timebucket.Top())
		require.Equal(t, day, timebucket.Of(p.maxTime, timebucket.Top()), "an output part fits one day")

		if s.capBytes > 0 && e.cfg.SideStore == nil {
			granuleBytes := int64(s.granule) * int64(maxRowBytes(t, p))
			require.LessOrEqual(t, p.sizeBytes(), s.capBytes+max(granuleBytes, s.capBytes/mergeRunFraction)+
				int64(maxRowBytes(t, p)), "a part overshoots its cap by at most one append")
		}

		checkPartMatchesBuffered(t, e, p)
	}
}

func maxRowBytes(t *testing.T, p *part) int {
	t.Helper()

	c, err := p.readCols(context.Background(), fullSel(p.schema), nil, nil)
	require.NoError(t, err)

	n := 0
	for i := range c.len() {
		n = max(n, int(c.rowBytes(i)+streamIDBytes))
	}

	return n
}

// checkPartMatchesBuffered rewrites p's rows with [writePart] at the merge's granule and compares
// the two parts object by object.
func checkPartMatchesBuffered(t *testing.T, e *Engine, p *part) {
	t.Helper()

	ctx := context.Background()
	be := e.cfg.Backend

	c, err := p.readCols(ctx, fullSel(p.schema), nil, nil)
	require.NoError(t, err)

	f := &flushColumns{cols: c}
	for _, r := range p.ranges {
		for range r.end - r.start {
			f.stream = append(f.stream, idToU128(r.id))
		}
	}

	prefix := e.newPartPrefix()
	require.NoError(t, writePart(ctx, be, e.cfg.Schema, prefix, f, e.identitiesForColumn(f.stream),
		e.cfg.MergeCompression, e.cfg.MergeCompressionLevel, e.blooms(), block.WithGranuleSize(e.mergeGranule)))

	q, err := openPart(ctx, be, e.cfg.Schema, prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)

	sm, bm := p.reader.Manifest(), q.reader.Manifest()
	assert.Equal(t, bm.RowCount, sm.RowCount)
	assert.Equal(t, bm.RawBytes, sm.RawBytes)
	assert.Equal(t, bm.GranuleSize, sm.GranuleSize)
	assert.Equal(t, [2]int64{bm.MinTime, bm.MaxTime}, [2]int64{sm.MinTime, sm.MaxTime})
	assert.Equal(t, q.ranges, p.ranges)

	qc, err := q.readCols(ctx, fullSel(q.schema), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, qc.ts, c.ts)
	assert.Equal(t, qc.ints, c.ints)

	for k := range c.bytes {
		for i := range c.len() {
			require.Equal(t, qc.bytes[k].at(i), c.bytes[k].at(i))
		}
	}

	for i := range sm.Columns {
		checkColumnMatchesBuffered(t, be, p, q, i)
	}

	sidecars := []string{identityKey(p.prefix), watermark.Key(p.prefix), recordKeysKey(p.prefix)}
	for k := range e.cfg.Schema.byteCols {
		if col := e.cfg.Schema.byteColumn(k); col.Bloom != BloomNone {
			sidecars = append(sidecars, bloomKey(p.prefix, col.Name))
		}
	}

	for _, key := range sidecars {
		got, gotErr := be.Read(ctx, key)
		wantObj, wantErr := be.Read(ctx, q.prefix+key[len(p.prefix):])
		require.Equal(t, wantErr == nil, gotErr == nil, key)
		assert.Equal(t, wantObj, got, "sidecar %s", key[len(p.prefix):])
	}
}

// checkColumnMatchesBuffered compares column i of the streamed part p with the buffered part q. A
// dictionary column that shares a dictionary is the same object either way; any other framed column
// holds the same frames, which the streamed writer puts ahead of its directory rather than after it,
// and the unframed stream id column is the same object.
// A dictionary column no granule of which joined is written unframed only by the buffered writer,
// which sees the whole column; it is compared decoded, as the caller already did.
func checkColumnMatchesBuffered(t *testing.T, be backend.Backend, p, q *part, i int) {
	t.Helper()

	ctx := context.Background()
	sd, bd := p.reader.Manifest().Columns[i], q.reader.Manifest().Columns[i]

	switch {
	case bd.Const:
		assert.Equal(t, bd, sd, "constant column %q", bd.Name)

		return
	case bd.Kind == block.KindBytes && !bd.Framed:
		assert.True(t, sd.TrailerDict, "a streamed all-decline column keeps the trailer layout: %q", bd.Name)
		assert.Zero(t, sd.DictEntries)

		return
	}

	sobj, err := be.Read(ctx, fmt.Sprintf("%s/c/%d", p.prefix, i))
	require.NoError(t, err)
	bobj, err := be.Read(ctx, fmt.Sprintf("%s/c/%d", q.prefix, i))
	require.NoError(t, err)

	if bd.TrailerDict || !bd.Blocked {
		assert.Equal(t, bobj, sobj, "column %q", bd.Name)

		return
	}

	sc, err := p.reader.Column(ctx, sd.Name)
	require.NoError(t, err)
	bc, err := q.reader.Column(ctx, bd.Name)
	require.NoError(t, err)

	sf, err := sc.Frames()
	require.NoError(t, err)
	bf, err := bc.Frames()
	require.NoError(t, err)
	require.Equal(t, bf, sf, "frames of %q", bd.Name)

	var frames int64
	for _, fr := range sf {
		frames += fr.Bytes
	}

	require.True(t, sd.Footer)
	assert.Equal(t, bobj[int64(len(bobj))-frames:], sobj[:frames], "frame bytes of %q", bd.Name)
}

func TestMergeStreamedMatchesBuffered(t *testing.T) {
	t.Parallel()

	for _, s := range []streamedShape{
		{seed: 1, parts: 3, streams: 3, rows: 200, granule: 8192},
		{seed: 2, parts: 4, streams: 2, rows: 300, granule: 16, capBytes: 8 << 10},
		{seed: 3, parts: 2, streams: 1, rows: 400, granule: 32, capBytes: 4 << 10, zstd: true},
		{seed: 4, parts: 3, streams: 4, rows: 120, granule: 8, capBytes: 6 << 10, days: 3},
		{seed: 5, parts: 3, streams: 3, rows: 150, granule: 64, retain: streamedDay + 40_000},
		{seed: 6, parts: 1, streams: 5, rows: 90, granule: 5, capBytes: 1 << 10, days: 2, zstd: true},
	} {
		t.Run(fmt.Sprintf("seed=%d", s.seed), func(t *testing.T) {
			t.Parallel()

			checkStreamedMatchesBuffered(t, s)
		})
	}
}

// FuzzMergeStreamedMatchesBuffered is [TestMergeStreamedMatchesBuffered] over generated shapes.
func FuzzMergeStreamedMatchesBuffered(f *testing.F) {
	f.Add(uint64(1), byte(3), byte(3), byte(80), byte(16), uint16(0), byte(0), false)
	f.Add(uint64(2), byte(2), byte(1), byte(200), byte(7), uint16(3), byte(20), true)
	f.Add(uint64(3), byte(4), byte(4), byte(40), byte(3), uint16(1), byte(90), false)

	f.Fuzz(func(t *testing.T, seed uint64, parts, streams, rows, granule byte, capKiB uint16, retain byte, zstd bool) {
		checkStreamedMatchesBuffered(t, streamedShape{
			seed:     seed,
			parts:    int(parts)%4 + 1,
			streams:  int(streams)%5 + 1,
			rows:     int(rows)%250 + 1,
			granule:  int(granule)%64 + 1,
			capBytes: int64(capKiB%64) << 10,
			retain:   streamedDay + int64(retain)*1000,
			days:     int(seed % 3),
			zstd:     zstd,
		})
	})
}

// TestPlanMergeBlocksSameStreamBothSides: a merge that seals inside a stream leaves that stream in
// every output part. The claim machinery never sees a stream id, so the parts take the split group's
// joint claim like any split; after the commit each carries the same claim over its own fresh block,
// and the stream's rows all read back, in order.
func TestPlanMergeBlocksSameStreamBothSides(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := New(Config{Schema: headTestSchema, Backend: backend.Memory(), Prefix: "t/midstream", MergeMemoryBytes: -1})
	e.mergeGranule = 16

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	want := make([]int64, 0, 300)

	for p := range 3 {
		b := &Batch{
			Stream: series.Hash(), Identity: func() signal.Series { return series },
			Ints: [][]int64{nil}, Bytes: [][][]byte{nil},
		}

		for i := range 100 {
			ts := int64(p*100+i) * 1000
			b.Ts = append(b.Ts, ts)
			b.Ints[0] = append(b.Ints[0], ts)
			b.Bytes[0] = append(b.Bytes[0], fmt.Appendf(nil, "record %06d with a body long enough to split", ts))
			want = append(want, ts)
		}

		_, err := e.AppendBatch(b, AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e.Flush(ctx))
	}

	src := e.parts
	require.Len(t, src, 3)

	out, err := e.compactParts(ctx, src, minInt64, 4<<10)
	require.NoError(t, err)
	require.Greater(t, len(out), 2, "the merge must seal inside the one stream more than once")

	for _, p := range out {
		require.Len(t, p.ranges, 1)
		assert.Equal(t, series.Hash(), p.ranges[0].id, "the same stream on both sides of every seal")
	}

	planMergeBlocks(src, out)

	var union bucketindex.Interval
	for _, p := range src {
		union = union.Union(p.blocks)
	}

	group := out[0].pending.group
	require.NotNil(t, group)
	assert.Equal(t, len(out), group.n)
	assert.Equal(t, union, group.blocks)

	for _, p := range out {
		assert.Same(t, group, p.pending.group, "one joint claim for every fragment")
		assert.False(t, p.pending.blocks.Valid(), "a fragment inherits no block of its own")
	}

	// The same shape through the engine: fragments committed, each with a fresh block and the joint
	// claim, and the stream still reads back whole.
	e2 := New(Config{Schema: headTestSchema, Backend: backend.Memory(), Prefix: "t/midstream",
		MaxPartBytes: 4 << 10, MergeMemoryBytes: -1})
	e2.mergeGranule, e2.mergeCap = 16, 4<<10

	for _, p := range src {
		c, err := p.readCols(ctx, fullSel(headTestSchema), nil, nil)
		require.NoError(t, err)

		b := &Batch{
			Stream: series.Hash(), Identity: func() signal.Series { return series },
			Ts: c.ts, Ints: c.ints, Bytes: [][][]byte{c.bytes[0].views(nil)},
		}
		_, err = e2.AppendBatch(b, AppendLimits{})
		require.NoError(t, err)
		require.NoError(t, e2.Flush(ctx))
	}

	require.NoError(t, e2.MergeWith(ctx, MergeOptions{Force: true}))

	var claims []string

	blocks := map[uint64]bool{}

	for _, p := range e2.parts {
		if p.level == 0 {
			continue
		}

		require.True(t, p.claim.Valid(), "a fragment carries the joint claim")
		claims = append(claims, fmt.Sprint(p.claim))
		require.Equal(t, p.blocks.Min, p.blocks.Max)
		assert.False(t, blocks[p.blocks.Min], "fragments take distinct blocks")
		blocks[p.blocks.Min] = true
	}

	require.Greater(t, len(claims), 1)
	assert.Len(t, slices.Compact(claims), 1, "every fragment carries the same claim")

	var got []int64

	for _, p := range e2.parts {
		c, err := p.readCols(ctx, fullSel(headTestSchema), nil, nil)
		require.NoError(t, err)

		got = append(got, c.ts...)
	}

	slices.Sort(got)
	assert.Equal(t, want, got)
}

func TestFinishRefusesEmptyPart(t *testing.T) {
	t.Parallel()

	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/empty"})

	w, err := newRecordPartStreamWriter(context.Background(), e, nil)
	require.NoError(t, err)

	_, err = w.finish(context.Background())
	require.Error(t, err)

	keys, err := be.List(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

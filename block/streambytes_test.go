package block

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// bytesFeed is how a test hands a bytes column's rows to a [StreamWriter].
type bytesFeed int

const (
	feedValues bytesFeed = iota
	feedBlob
	feedBind
	feedBindStable
	feedCount
)

func (f bytesFeed) String() string {
	return [...]string{"values", "blob", "bind", "bind stable"}[f]
}

type bytesCase struct {
	name  string
	vals  [][]byte
	gsize int
}

func valuesOf(n int, cell func(i int) string) [][]byte {
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = []byte(cell(i))
	}

	return vals
}

// bytesCases span the granule decisions: every granule joining, every one declining, a mix, the
// 256→257 width transition, a constant, empty values, one-row granules (which never join) and
// values past an arena chunk.
func bytesCases() []bytesCase {
	big := func(i int) string {
		if i%11 == 0 {
			return string(make([]byte, arenaChunkBytes+i))
		}

		return fmt.Sprintf("v-%d", i%5)
	}

	return []bytesCase{
		{"low cardinality", valuesOf(300, func(i int) string { return fmt.Sprintf("attr-%d", i%7) }), 16},
		{"unique", valuesOf(100, func(i int) string { return fmt.Sprintf("body-%d", i) }), 8},
		{"mixed", valuesOf(160, func(i int) string {
			if (i/16)%3 == 1 {
				return fmt.Sprintf("u-%d", i)
			}

			return fmt.Sprintf("s-%d", i%3)
		}), 16},
		{"transition", transitionValues(), transitionGranule},
		{"constant", valuesOf(50, func(int) string { return "same" }), 8},
		{"single row", valuesOf(1, func(int) string { return "x" }), 4},
		{"empty values", valuesOf(64, func(i int) string {
			if i%2 == 0 {
				return ""
			}

			return fmt.Sprintf("e-%d", i%4)
		}), 8},
		{"one-row granules", valuesOf(12, func(i int) string { return fmt.Sprintf("r-%d", i%3) }), 1},
		{"large values", valuesOf(40, big), 8},
	}
}

func tsRows(n int) []int64 {
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i) * 1000
	}

	return ts
}

// batchBytesPart builds a ts column and the bytes column with [PartWriter].
func batchBytesPart(tb testing.TB, vals [][]byte, codec chunk.Codec, obs BytesObserver, opts ...PartOption) builtPart {
	tb.Helper()

	w := NewPartWriter(opts...)
	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: tsRows(len(vals)), Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "b", Kind: KindBytes, Codec: codec, Bytes: vals, Block: true, Observer: obs}))

	built, err := w.build()
	require.NoError(tb, err)

	return built
}

// streamBytesPart declares the same schema on w and feeds it vals in random batches through feed.
func streamBytesPart(
	tb testing.TB, w *StreamWriter, vals [][]byte, codec chunk.Codec, feed bytesFeed, obs BytesObserver, seed uint64,
) {
	tb.Helper()

	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "b", Kind: KindBytes, Codec: codec, Block: true, Observer: obs}))
	require.NoError(tb, w.AppendInt64(0, tsRows(len(vals))))

	rng := rand.New(rand.NewPCG(seed, seed^0x5bd1e995))
	src := newBindSource(tb, w, feed, vals)

	for lo := 0; lo < len(vals); {
		hi := min(len(vals), lo+1+rng.IntN(23))
		src.feed(vals[lo:hi], rng)
		lo = hi
	}
}

// bindSource plays a merge source: it presents rows as dictionary granules through a shared and a
// self binding, the way a cursor over a decoded part does.
type bindSource struct {
	tb        testing.TB
	w         *StreamWriter
	how       bytesFeed
	shared    *Binding
	self      *Binding
	sharedTab [][]byte
	sharedIdx map[string]int
	sharedGen DictGen
}

func newBindSource(tb testing.TB, w *StreamWriter, how bytesFeed, vals [][]byte) *bindSource {
	tb.Helper()

	s := &bindSource{tb: tb, w: w, how: how}
	if how != feedBind && how != feedBindStable {
		return s
	}

	var err error

	s.shared, err = w.Binding(1)
	require.NoError(tb, err)
	s.self, err = w.Binding(1)
	require.NoError(tb, err)

	s.sharedIdx = map[string]int{}

	for _, v := range vals {
		if _, ok := s.sharedIdx[string(v)]; !ok && len(s.sharedTab) < 65536 {
			s.sharedIdx[string(v)] = len(s.sharedTab)
			s.sharedTab = append(s.sharedTab, v)
		}
	}

	s.sharedGen = NewDictGen()

	if how == feedBindStable {
		require.NoError(tb, s.shared.BindStable(s.sharedTab, s.sharedGen))
	} else {
		require.NoError(tb, s.shared.Bind(s.sharedTab, s.sharedGen))
	}

	return s
}

func (s *bindSource) feed(rows [][]byte, rng *rand.Rand) {
	tb := s.tb

	switch s.how {
	case feedValues:
		require.NoError(tb, s.w.AppendBytes(1, rows))
	case feedBlob:
		blob, offsets := []byte("prefix"), make([]int32, 1, 1+len(rows))
		offsets[0] = 6
		for _, v := range rows {
			blob = append(blob, v...)
			offsets = append(offsets, int32(len(blob)))
		}

		require.NoError(tb, s.w.AppendBytesBlob(1, blob, offsets))
	default:
		s.feedDict(rows, rng)
	}
}

// feedDict wraps rows in a decoded granule with a filler row between every two, which keep drops:
// on the shared table, on a table of its own, or flat.
func (s *bindSource) feedDict(rows [][]byte, rng *rand.Rand) {
	tb := s.tb

	switch rng.IntN(3) {
	case 0:
		cells, keep := interleave(rows, s.sharedTab[0])
		dc := idsColumn(s.sharedTab, cells, func(v []byte) int { return s.sharedIdx[string(v)] })
		require.NoError(tb, s.shared.AppendDict(OwnedGranule(dc, s.sharedGen), 0, len(cells), keep))
	case 1:
		cells, keep := interleave(rows, []byte("filler-never-kept"))
		entries, _ := splitBytesForm(cells)

		idx := map[string]int{}
		for i, e := range entries {
			idx[string(e)] = i
		}

		gen := NewDictGen()
		require.NoError(tb, s.self.Bind(entries, gen))
		require.NoError(tb, s.self.AppendDict(OwnedGranule(
			idsColumn(entries, cells, func(v []byte) int { return idx[string(v)] }), gen), 0, len(cells), keep))
	default:
		cells, keep := interleave(rows, []byte("filler-never-kept"))
		gen := NewDictGen()
		require.NoError(tb, s.self.Bind(cells, gen))
		require.NoError(tb, s.self.AppendDict(OwnedGranule(&chunk.DictColumn{Entries: cells}, gen), 0, len(cells), keep))
	}
}

func interleave(rows [][]byte, filler []byte) (cells [][]byte, keep []bool) {
	for _, v := range rows {
		cells = append(cells, filler, v)
		keep = append(keep, false, true)
	}

	return cells, keep
}

// idsColumn encodes rows as ids into tab at the narrowest width that holds it.
func idsColumn(tab, rows [][]byte, idx func([]byte) int) *chunk.DictColumn {
	dc := &chunk.DictColumn{Entries: tab, IDWidth: 1}
	if len(tab) > 256 {
		dc.IDWidth = 2
	}

	for _, v := range rows {
		id := idx(v)
		if dc.IDWidth == 1 {
			dc.IDs = append(dc.IDs, byte(id))
		} else {
			dc.IDs = binary.BigEndian.AppendUint16(dc.IDs, uint16(id))
		}
	}

	return dc
}

func buildStream(tb testing.TB, w *StreamWriter) builtPart {
	tb.Helper()

	built, err := w.build()
	require.NoError(tb, err)

	return built
}

// readBytesColumn reads the bytes column of a part through the whole-object paths.
func readBytesColumn(t *testing.T, ctx context.Context, b backend.Backend, prefix string, want [][]byte) {
	t.Helper()

	r, err := OpenPart(ctx, b, prefix)
	require.NoError(t, err)

	desc, ok := r.ColumnDescByName("b")
	require.True(t, ok)

	col, err := r.Column(ctx, "b")
	require.NoError(t, err)

	if desc.Codec == chunk.CodecBytesRaw || desc.Const {
		got, err := col.Bytes()
		require.NoError(t, err)
		require.Equal(t, len(want), got.Len())

		// The raw codec decodes an empty value as nil.
		for i, v := range want {
			require.Truef(t, bytes.Equal(v, got.At(i)), "row %d", i)
		}

		return
	}

	obj, err := b.Read(ctx, columnKey(prefix, 1))
	require.NoError(t, err)
	readAllPaths(t, desc, obj, col.comp, want)

	if !desc.Blocked {
		return
	}

	scan, err := r.ColumnScan(ctx, "b", 1<<10)
	require.NoError(t, err)
	checkDecoderRows(t, scan, want, "scan")
}

// normalizedColumn digests a framed column independently of where its directory sits: the
// directory fields, every frame's bytes and the dictionary region.
func normalizedColumn(t *testing.T, desc ColumnDesc, obj []byte) string {
	t.Helper()

	d, err := parseBlockDir(obj, desc)
	require.NoError(t, err)

	h := sha256.New()
	put := func(v ...int64) {
		for _, x := range v {
			_ = binary.Write(h, binary.LittleEndian, x)
		}
	}

	put(int64(d.blockRows), int64(d.granules), int64(len(d.frameOff)))

	for f := range len(d.frameOff) - 1 {
		frame, err := d.frame(f)
		require.NoError(t, err)

		put(int64(len(frame)), int64(d.frameRaw[f]))
		h.Write(frame)
	}

	for g := range d.granules {
		put(int64(d.gFrame[g]), int64(d.gOff[g]), int64(d.gLen[g]))
	}

	if desc.TrailerDict {
		h.Write(obj[desc.DictOff : desc.DictOff+desc.DictLen])
	}

	return hex.EncodeToString(h.Sum(nil))
}

func streamOpts(gsize int, alg compress.Algorithm, extra ...PartOption) []PartOption {
	return append([]PartOption{
		WithSortKey("ts"), WithGranuleSize(gsize), WithCompression(alg), WithCompressBlockBytes(64), WithSizingStats(),
	}, extra...)
}

// TestStreamWriterBytesMatchesPartWriter is the parity invariant for bytes columns. A buffered
// writer's part is byte-identical to [PartWriter]'s. A streamed writer's trailer column is too; a
// streamed raw column differs only in where its directory sits; and a streamed column no granule
// joined keeps the trailer layout where the others write one stream, so it is compared by rows.
func TestStreamWriterBytesMatchesPartWriter(t *testing.T) {
	t.Parallel()

	for _, tc := range bytesCases() {
		for _, codec := range []chunk.Codec{chunk.CodecDict, chunk.CodecBytesRaw} {
			for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
				for feed := range feedCount {
					name := fmt.Sprintf("%s/%s/%s/%s", tc.name, codec, alg, feed)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						checkBytesParity(t, tc.vals, codec, feed, streamOpts(tc.gsize, alg), 1)
					})
				}
			}
		}
	}
}

func checkBytesParity(t *testing.T, vals [][]byte, codec chunk.Codec, feed bytesFeed, opts []PartOption, seed uint64) {
	t.Helper()

	ctx := context.Background()

	batch := batchBytesPart(t, vals, codec, nil, opts...)

	w := NewStreamWriter(opts...)
	streamBytesPart(t, w, vals, codec, feed, nil, seed)
	buffered := buildStream(t, w)

	require.Equal(t, batch.manifest, buffered.manifest, "manifest")
	require.Equal(t, batch.marks, buffered.marks, "marks")
	require.Equal(t, batch.objects, buffered.objects, "objects")

	sb := backendtest.NewStreamingMemory()
	sw := NewStreamWriterTo(ctx, sb, "s", opts...)

	defer sw.Abort()

	streamBytesPart(t, sw, vals, codec, feed, nil, seed)
	require.NoError(t, WriteStreamPart(ctx, sb, "s", sw))

	mb := backend.Memory()
	require.NoError(t, batch.write(ctx, mb, "b"))

	readBytesColumn(t, ctx, sb, "s", vals)
	readBytesColumn(t, ctx, mb, "b", vals)

	want, err := DecodeManifest(batch.manifest)
	require.NoError(t, err)

	sr, err := OpenPart(ctx, sb, "s")
	require.NoError(t, err)

	got := sr.Manifest()
	wd, gd := want.Columns[1], got.Columns[1]

	switch {
	case wd.Const || len(vals) == 0:
		assert.Equal(t, wd, gd, "a column with no granule never attaches, so it is written buffered")
	case wd.TrailerDict:
		assert.Equal(t, wd, gd)

		obj, err := sb.Read(ctx, columnKey("s", 1))
		require.NoError(t, err)
		assert.Equal(t, batch.objects[1], obj, "a trailer column is written the same streamed or buffered")
	case codec == chunk.CodecDict:
		assert.True(t, gd.TrailerDict, "a streamed all-decline column keeps the trailer layout")
		assert.Zero(t, gd.DictEntries)
	default:
		obj, err := sb.Read(ctx, columnKey("s", 1))
		require.NoError(t, err)

		assert.True(t, gd.Footer)
		assert.Equal(t, normalizedColumn(t, wd, batch.objects[1]), normalizedColumn(t, gd, obj))

		gd.Footer, gd.Bytes = false, wd.Bytes
		gd.Sizing.DirLen = wd.Sizing.DirLen
		assert.Equal(t, wd, gd)
	}

	assert.Equal(t, want.RawBytes, got.RawBytes)
	assert.Equal(t, want.RowCount, got.RowCount)
}

// TestStreamWriterBytesAllDeclineMatchesFramedTrailer pins the streamed all-decline layout: the
// trailer column with an empty dictionary that the batch encoder writes when told to frame.
func TestStreamWriterBytesAllDeclineMatchesFramedTrailer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vals := valuesOf(3*transitionGranule, func(i int) string { return fmt.Sprintf("unique-%d", i) })

	b := backendtest.NewStreamingMemory()
	w := NewStreamWriterTo(ctx, b, "p", WithGranuleSize(transitionGranule), WithCompressBlockBytes(defaultCompressBlockBytes))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
	require.NoError(t, w.AppendBytes(0, vals))
	require.NoError(t, WriteStreamPart(ctx, b, "p", w))

	obj, err := b.Read(ctx, columnKey("p", 0))
	require.NoError(t, err)

	want, _, _, ok, err := encodeTrailerDictBytes(Column{Name: "b", Kind: KindBytes, Bytes: vals, Block: true},
		noneComp(), defaultLayout(transitionGranule), true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, want, obj)
	assert.Equal(t, "e583175cb606d243", digest(obj), "the trailer golden's zero-entry column")
}

// recordingObserver records every call, copying what it is shown since slices alias writer memory.
type recordingObserver struct {
	calls []observed
}

type observed struct {
	dict   bool
	values []string
	counts []uint64
}

func (o *recordingObserver) SelfGranule(values [][]byte, counts []uint64) {
	o.record(false, values, counts)
}
func (o *recordingObserver) Dictionary(entries [][]byte, counts []uint64) {
	o.record(true, entries, counts)
}

func (o *recordingObserver) record(dict bool, values [][]byte, counts []uint64) {
	c := observed{dict: dict, counts: append([]uint64(nil), counts...)}
	for _, v := range values {
		c.values = append(c.values, string(v))
	}

	o.calls = append(o.calls, c)
}

// check asserts the observer contract against the column's rows: the dictionary reported once and
// last, every value at least once, and the counts summing to the rows and to each value's frequency.
func (o *recordingObserver) check(t *testing.T, vals [][]byte) {
	t.Helper()

	require.NotEmpty(t, o.calls)

	for i, c := range o.calls {
		assert.Equal(t, i == len(o.calls)-1, c.dict, "call %d", i)
		require.Len(t, c.counts, len(c.values))
	}

	freq := map[string]uint64{}
	for _, v := range vals {
		freq[string(v)]++
	}

	got := map[string]uint64{}

	for _, c := range o.calls {
		for i, v := range c.values {
			got[v] += c.counts[i]
		}
	}

	assert.Equal(t, freq, got)
}

// TestBytesObserver checks the observer contract for both writers, and that they report the same
// calls: a buffered stream makes the same granule decisions as the batch writer.
func TestBytesObserver(t *testing.T) {
	t.Parallel()

	for _, tc := range bytesCases() {
		if tc.name == "constant" || tc.name == "single row" {
			continue // a constant column collapses into the manifest
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for feed := range feedCount {
				opts := streamOpts(tc.gsize, compress.AlgorithmNone)

				var batch, stream, streamed recordingObserver

				batchBytesPart(t, tc.vals, chunk.CodecDict, &batch, opts...)
				batch.check(t, tc.vals)

				var split recordingObserver

				entries, ids := splitBytesForm(tc.vals)
				pw := NewPartWriter(opts...)
				require.NoError(t, pw.AddColumn(Column{
					Name: "b", Kind: KindBytes, BytesDict: entries, BytesIDs: ids, Block: true, Observer: &split,
				}))
				_, err := pw.build()
				require.NoError(t, err)
				assert.Equal(t, batch.calls, split.calls, "split form")

				w := NewStreamWriter(opts...)
				streamBytesPart(t, w, tc.vals, chunk.CodecDict, feed, &stream, 3)
				buildStream(t, w)
				assert.Equal(t, batch.calls, stream.calls, feed.String())

				ctx := context.Background()
				b := backendtest.NewStreamingMemory()
				sw := NewStreamWriterTo(ctx, b, "p", opts...)
				streamBytesPart(t, sw, tc.vals, chunk.CodecDict, feed, &streamed, 3)
				require.NoError(t, WriteStreamPart(ctx, b, "p", sw))
				streamed.check(t, tc.vals)
			}
		})
	}
}

// TestBytesObserverRejectedOffDictColumns pins where an observer is valid.
func TestBytesObserverRejectedOffDictColumns(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}

	for _, c := range []Column{
		{Name: "raw", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Block: true, Observer: obs},
		{Name: "unblocked", Kind: KindBytes, Observer: obs},
		{Name: "int", Kind: KindInt64, Codec: chunk.CodecT64, Block: true, Observer: obs},
	} {
		require.Error(t, NewPartWriter().AddColumn(c), c.Name)
		require.Error(t, NewStreamWriter().AddColumn(c), c.Name)
	}

	w := NewPartWriter(WithGranuleSize(0))
	require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Bytes: valuesOf(2, strconv.Itoa), Block: true, Observer: obs}))
	_, err := w.build()
	require.Error(t, err, "an observed column needs granules to report")
}

// TestStreamWriterBytesRejects covers the guard rails the bytes path adds.
func TestStreamWriterBytesRejects(t *testing.T) {
	t.Parallel()

	t.Run("codec", func(t *testing.T) {
		t.Parallel()

		require.Error(t, NewStreamWriter().AddColumn(Column{Name: "b", Kind: KindBytes, Codec: chunk.CodecT64, Block: true}))
	})

	t.Run("granule size", func(t *testing.T) {
		t.Parallel()

		require.Error(t, NewStreamWriter(WithGranuleSize(0)).AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
	})

	t.Run("kind", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
		require.Error(t, w.AppendBytes(0, nil))
		require.Error(t, w.AppendBytesBlob(0, nil, nil))

		_, err := w.Binding(0)
		require.Error(t, err)
	})

	t.Run("blob offsets", func(t *testing.T) {
		t.Parallel()

		w := NewStreamWriter()
		require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
		require.Error(t, w.AppendBytesBlob(0, []byte("abc"), []int32{0, 4}))
		require.Error(t, w.AppendBytesBlob(0, []byte("abc"), []int32{2, 1}))
		require.Error(t, w.AppendBytesBlob(0, []byte("abc"), []int32{-1, 1}))
		require.NoError(t, w.AppendBytesBlob(0, []byte("abc"), []int32{0}), "no rows")
	})

	for _, codec := range []chunk.Codec{chunk.CodecDict, chunk.CodecBytesRaw} {
		t.Run("after finish "+codec.String(), func(t *testing.T) {
			t.Parallel()

			w := NewStreamWriter()
			require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Codec: codec, Block: true}))
			require.NoError(t, w.AppendBytes(0, valuesOf(3, strconv.Itoa)))

			bnd, err := w.Binding(0)
			require.NoError(t, err)

			buildStream(t, w)

			require.ErrorIs(t, w.AppendBytes(0, valuesOf(1, strconv.Itoa)), errWriterFinished)
			require.ErrorIs(t, bnd.Bind(nil, NewDictGen()), errWriterFinished)

			_, err = w.Binding(0)
			require.ErrorIs(t, err, errWriterFinished)
		})
	}
}

// TestStreamWriterDictCap pins the cap on the streaming side, against the batch writer: charged only
// for values a granule adds, so a granule repeating a 17 MiB value already in D still joins with its
// new 1-byte value; a cap below the value declines it; a zero cap declines everything. Under zstd the
// 17 MiB zero-filled dictionary compresses far past any ratio a reader could have assumed.
func TestStreamWriterDictCap(t *testing.T) {
	t.Parallel()

	a, bv := make([]byte, 17<<20), []byte("b")
	vals := [][]byte{a, a, a, a, a, a, a, bv}

	for _, tc := range []struct {
		name    string
		cap     int64
		entries int64
		framed  bool
	}{
		{"default cap", defaultSharedDictBytes, 2, true},
		{"cap below the value", 17 << 20, 0, false},
		{"zero cap", 0, 0, false},
	} {
		for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
			for _, feed := range []bytesFeed{feedValues, feedBindStable} {
				t.Run(fmt.Sprintf("%s/%s/%s", tc.name, alg, feed), func(t *testing.T) {
					t.Parallel()

					opts := streamOpts(4, alg, WithSharedDictBytes(tc.cap))
					checkBytesParity(t, vals, chunk.CodecDict, feed, opts, 2)

					m, err := DecodeManifest(batchBytesPart(t, vals, chunk.CodecDict, nil, opts...).manifest)
					require.NoError(t, err)
					assert.Equal(t, tc.framed, m.Columns[1].Framed)
					assert.Equal(t, tc.entries, m.Columns[1].DictEntries)
				})
			}
		}
	}
}

// TestStreamWriterToBytesResidentBytesStaysFlat is the bytes counterpart of the metric test: with
// frames drained and the dictionary bounded by its distinct values, a streamed bytes column's
// footprint stops tracking the part.
func TestStreamWriterToBytesResidentBytesStaysFlat(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct {
		name string
		cell func(i int) string
	}{
		{"shared", func(i int) string { return fmt.Sprintf("svc=api pod=worker-%03d level=info", i%512) }},
		{"selfencoded", func(i int) string { return fmt.Sprintf("request %d completed in %dms", i, i%977) }},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			opts := []PartOption{WithGranuleSize(8192), WithCompressBlockBytes(64 << 10)}

			streamed := NewStreamWriterTo(ctx, backendtest.NewStreamingMemory(), "p", opts...)
			defer streamed.Abort()

			buffered := NewStreamWriter(opts...)

			for _, w := range []*StreamWriter{streamed, buffered} {
				require.NoError(t, w.AddColumn(Column{Name: "b", Kind: KindBytes, Block: true}))
				require.NoError(t, w.AddColumn(Column{Name: "r", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Block: true}))
			}

			// rawAfter is the value bytes the raw column takes after the early sample. Uncompressed, a
			// buffered writer holds every one of them, whatever the other column's frames compress to.
			var early, earlyBuffered, rawAfter int64

			const batches = 256

			for n := range batches {
				vals := valuesOf(4096, func(i int) string { return shape.cell(n*4096 + i) })

				for _, w := range []*StreamWriter{streamed, buffered} {
					require.NoError(t, w.AppendBytes(0, vals))
					require.NoError(t, w.AppendBytes(1, vals))
				}

				switch {
				case n == batches/8:
					early, earlyBuffered = streamed.ResidentBytes(), buffered.ResidentBytes()
				case n > batches/8:
					for _, v := range vals {
						rawAfter += int64(len(v))
					}
				}
			}

			require.Positive(t, early)
			assert.Less(t, streamed.ResidentBytes(), early*3/2, "a streamed bytes column must not grow with the part")
			assert.GreaterOrEqual(t, buffered.ResidentBytes()-earlyBuffered, rawAfter,
				"a buffered writer holds the part it builds, the raw column's values at least")
		})
	}
}

// TestByteArena pins that a copy survives the arena growing and that reset keeps only chunks of the
// standard size.
func TestByteArena(t *testing.T) {
	t.Parallel()

	var a byteArena

	assert.Nil(t, a.copy(nil))

	small := a.copy([]byte("abc"))
	big := a.copy(make([]byte, arenaChunkBytes+1))
	after := a.copy([]byte("def"))

	assert.Equal(t, []byte("abc"), small)
	assert.Len(t, big, arenaChunkBytes+1)
	assert.Equal(t, []byte("def"), after)
	assert.Len(t, a.chunks, 3, "a value too long for the chunk in use gets its own, and the next opens a new one")
	assert.Equal(t, 3, cap(small), "copies are capped so an append cannot run into the next one")

	a.reset()
	assert.Len(t, a.chunks, 2)
	assert.Positive(t, a.size())

	a.release()
	assert.Zero(t, a.size())
}

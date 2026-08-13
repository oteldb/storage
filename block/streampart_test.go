package block

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// streamingMemory is [backend.Memory] with the incremental-write capability bolted on. The real
// implementation is the file backend's; this one exists so the streamed layout — and the code that
// only runs under it — is exercised everywhere the package already tests in memory.
type streamingMemory struct {
	backend.Backend
}

func newStreamingMemory() *streamingMemory { return &streamingMemory{Backend: backend.Memory()} }

func (b *streamingMemory) CreateObject(_ context.Context, key string) (backend.ObjectWriter, error) {
	return &memoryObjectWriter{b: b.Backend, key: key}, nil
}

type memoryObjectWriter struct {
	b   backend.Backend
	key string
	buf []byte
}

func (w *memoryObjectWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)

	return len(p), nil
}

func (w *memoryObjectWriter) Commit(ctx context.Context) error { return w.b.Write(ctx, w.key, w.buf) }
func (w *memoryObjectWriter) Abort()                           { w.buf = nil }

// writeStreamTo builds the part with a streaming writer targeting b, so its column objects are
// committed through the backend rather than returned.
func (m metricRows) writeStreamTo(
	tb testing.TB, ctx context.Context, b backend.Backend, prefix string, autoCodec bool, opts ...PartOption,
) {
	tb.Helper()

	w := NewStreamWriterTo(ctx, b, prefix, opts...)
	defer w.Abort()

	require.NoError(tb, w.AddColumn(Column{Name: "series", Kind: KindInt128}))
	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(tb, w.AddColumn(Column{
		Name: "value", Kind: KindFloat64, AutoCodec: autoCodec, Block: true,
	}))

	off := 0
	for _, r := range m.runs() {
		require.NoError(tb, w.AppendU128Run(0, r.Value, r.Count))
		require.NoError(tb, w.AppendInt64(1, m.ts[off:off+r.Count]))
		require.NoError(tb, w.AppendFloat64(2, m.value[off:off+r.Count]))
		off += r.Count
	}

	require.NoError(tb, WriteStreamPart(ctx, b, prefix, w))
}

// readPart reads the three metric columns of the part under prefix.
func readPart(tb testing.TB, ctx context.Context, b backend.Backend, prefix string) (metricRows, Manifest) {
	tb.Helper()

	r, err := OpenPart(ctx, b, prefix)
	require.NoError(tb, err)

	var got metricRows

	sc, err := r.Column(ctx, "series")
	require.NoError(tb, err)
	got.series, err = sc.ID128(nil)
	require.NoError(tb, err)

	tc, err := r.Column(ctx, "ts")
	require.NoError(tb, err)
	got.ts, err = tc.Int64(nil)
	require.NoError(tb, err)

	vc, err := r.Column(ctx, "value")
	require.NoError(tb, err)
	got.value, err = vc.Float64(nil)
	require.NoError(tb, err)

	return got, r.Manifest()
}

// TestStreamWriterToMatchesBuffered is the invariant a streamed part rests on: draining frames to
// the backend as they seal changes where the directory sits, and nothing else. The rows must decode
// identically and the manifest must agree field for field apart from the layout flag.
func TestStreamWriterToMatchesBuffered(t *testing.T) {
	t.Parallel()

	for _, tc := range metricCases() {
		if len(tc.rows.ts) == 0 {
			continue // a part with no columns is rejected before any object is opened
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, autoCodec := range []bool{false, true} {
				t.Run(codecName(autoCodec), func(t *testing.T) {
					t.Parallel()

					ctx := context.Background()
					opts := []PartOption{
						WithSortKey("ts"), WithGranuleSize(tc.gsize),
						WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64),
					}

					b := newStreamingMemory()
					tc.rows.writeStreamTo(t, ctx, b, "streamed", autoCodec, opts...)

					buffered := backend.Memory()
					require.NoError(t, tc.rows.writeStream(t, autoCodec, opts...).write(ctx, buffered, "buffered"))

					got, gotManifest := readPart(t, ctx, b, "streamed")
					want, wantManifest := readPart(t, ctx, buffered, "buffered")

					assert.Equal(t, want.series, got.series, "series")
					assert.Equal(t, want.ts, got.ts, "ts")
					assertFloatsEqual(t, want.value, got.value)

					assertManifestsAgree(t, wantManifest, gotManifest)
				})
			}
		})
	}
}

func codecName(autoCodec bool) string {
	if autoCodec {
		return "auto codec"
	}

	return "explicit codec"
}

// assertManifestsAgree compares the two manifests field for field, allowing only the differences the
// layout implies: the footer flag, and the object sizes it shifts by four bytes per streamed column.
func assertManifestsAgree(t *testing.T, want, got Manifest) {
	t.Helper()

	require.Len(t, got.Columns, len(want.Columns))

	streamed := 0

	for i := range want.Columns {
		w, g := want.Columns[i], got.Columns[i]
		if g.Footer {
			streamed++

			assert.True(t, g.Framed, "column %q: a footer directory is a framed one", g.Name)

			// The footer layout costs the 4-byte directory length and nothing else.
			g.Footer = false
			g.Bytes -= footerLenBytes
		}

		assert.Equal(t, canonicalNaN(w), canonicalNaN(g), "column %d descriptor", i)
	}

	assert.Equal(t, want.DiskBytes+int64(streamed)*footerLenBytes, got.DiskBytes, "disk bytes")

	want.Columns, got.Columns = nil, nil
	want.DiskBytes, got.DiskBytes = 0, 0

	assert.Equal(t, want, got, "manifest")
}

// canonicalNaN replaces a descriptor's NaN floats with a sentinel, so two descriptors that agree on
// "no meaningful range" compare equal: a NaN never equals itself, and an all-NaN column stamps three
// of them.
func canonicalNaN(c ColumnDesc) ColumnDesc {
	const sentinel = -1

	for _, f := range []*float64{&c.ConstFloat64, &c.MinFloat64, &c.MaxFloat64} {
		if math.IsNaN(*f) {
			*f = sentinel
		}
	}

	return c
}

// assertFloatsEqual compares by bit pattern, so NaN payloads and negative zero are held to the same
// standard the codecs are.
func assertFloatsEqual(t *testing.T, want, got []float64) {
	t.Helper()

	require.Len(t, got, len(want))

	for i := range want {
		assert.Equal(t, math.Float64bits(want[i]), math.Float64bits(got[i]), "value %d", i)
	}
}

// TestStreamWriterToDiskBytesMatchesObjects checks the manifest's byte accounting against the
// objects actually stored: a streamed column never returns its bytes, so its size is reported rather
// than measured, and a merge's seal threshold reads that number.
func TestStreamWriterToDiskBytesMatchesObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(20, 40, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 42)

	rows.writeStreamTo(t, ctx, b, "p", true,
		WithSortKey("ts"), WithGranuleSize(8), WithCompressBlockBytes(64))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	keys, err := b.List(ctx, "p/")
	require.NoError(t, err)

	var total int64

	for _, k := range keys {
		if k == manifestKey("p") {
			continue // the manifest carries the count, so it cannot be part of it
		}

		n, err := backend.SizeOf(ctx, b, k)
		require.NoError(t, err)

		total += n
	}

	assert.Equal(t, total, r.Manifest().DiskBytes)
}

// TestStreamWriterToConstColumnStaysUnwritten pins the reason a column cannot stream from its first
// granule: a column that turns out constant collapses into the manifest and must have no object at
// all, which is only knowable once every row has been seen.
func TestStreamWriterToConstColumnStaysUnwritten(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(4, 20, func(*rand.Rand, int, int) float64 { return 7 }, 1)

	rows.writeStreamTo(t, ctx, b, "p", false, WithSortKey("ts"), WithGranuleSize(4))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	desc, ok := r.ColumnDescByName("value")
	require.True(t, ok)
	require.True(t, desc.Const, "an all-7 column must collapse")
	assert.False(t, desc.Footer)

	_, err = b.Read(ctx, columnKey("p", 2))
	require.ErrorIs(t, err, backend.ErrNotExist, "a collapsed column must leave no object")

	got, _ := readPart(t, ctx, b, "p")
	assert.Equal(t, rows.value, got.value)
}

// TestStreamWriterToAbortLeavesNothing covers the merge path that gives up partway: the objects a
// part had under way must not survive it, or every abandoned merge strands a prefix's worth of them.
func TestStreamWriterToAbortLeavesNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(10, 50, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 3)

	w := NewStreamWriterTo(ctx, b, "p", WithSortKey("ts"), WithGranuleSize(8), WithCompressBlockBytes(64))
	require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
	require.NoError(t, w.AddColumn(Column{Name: "value", Kind: KindFloat64, AutoCodec: true, Block: true}))
	require.NoError(t, w.AppendInt64(0, rows.ts))
	require.NoError(t, w.AppendFloat64(1, rows.value))

	w.Abort()
	w.Abort() // idempotent: the merge path aborts from a defer that also runs after a clean finish

	keys, err := b.List(ctx, "p")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestStreamWriterToResidentBytesStaysFlat is the point of the whole exercise: a streamed part's
// resident footprint must stop tracking the part. A buffered writer's grows with it.
func TestStreamWriterToResidentBytesStaysFlat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	// The production granule and frame sizes: the directory a streamed part keeps is O(granules), so
	// a test at a toy granule size would measure that term instead of the one this change removes.
	opts := []PartOption{WithSortKey("ts"), WithGranuleSize(8192), WithCompressBlockBytes(64 << 10)}

	streamed := NewStreamWriterTo(ctx, b, "p", opts...)
	defer streamed.Abort()

	buffered := NewStreamWriter(opts...)

	for _, w := range []*StreamWriter{streamed, buffered} {
		require.NoError(t, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Block: true}))
		require.NoError(t, w.AddColumn(Column{Name: "value", Kind: KindFloat64, Block: true}))
	}

	rng := rand.New(rand.NewPCG(7, 8))

	var (
		earlyStreamed, earlyBuffered int64
		batches                      = 512
	)

	for n := range batches {
		ts := make([]int64, 4096)
		values := make([]float64, len(ts))

		for i := range ts {
			ts[i] = int64(n*len(ts)+i) * 15_000
			values[i] = rng.Float64()
		}

		for _, w := range []*StreamWriter{streamed, buffered} {
			require.NoError(t, w.AppendInt64(0, ts))
			require.NoError(t, w.AppendFloat64(1, values))
		}

		if n == batches/8 {
			earlyStreamed, earlyBuffered = streamed.ResidentBytes(), buffered.ResidentBytes()
		}
	}

	require.Positive(t, earlyStreamed)
	assert.Less(t, streamed.ResidentBytes(), 2*earlyStreamed,
		"a streamed part's footprint must not grow with the part")
	assert.Greater(t, buffered.ResidentBytes(), 4*earlyBuffered,
		"a buffered part's footprint does grow with it — the bound this change removes")
	assert.Less(t, streamed.ResidentBytes(), buffered.ResidentBytes()/8)
}

// TestParseFooterDirRejectsCorrupt covers the fields only the footer layout has: a reader locates
// the directory from the object's end, so a bad length must error rather than slice out of range.
func TestParseFooterDirRejectsCorrupt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(3, 40, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 11)

	rows.writeStreamTo(t, ctx, b, "p", false, WithSortKey("ts"), WithGranuleSize(8), WithCompressBlockBytes(64))

	object, err := b.Read(ctx, columnKey("p", 1))
	require.NoError(t, err)
	require.NotEmpty(t, object)

	_, err = parseBlockDir(object, true, true)
	require.NoError(t, err, "the unmodified object must parse")

	dirLen := binary.LittleEndian.Uint32(object[len(object)-footerLenBytes:])

	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"shorter than the trailer", func(o []byte) []byte { return o[:2] }},
		{"directory longer than the object", func(o []byte) []byte {
			binary.LittleEndian.PutUint32(o[len(o)-footerLenBytes:], uint32(len(o)))

			return o
		}},
		{"directory truncated", func(o []byte) []byte {
			binary.LittleEndian.PutUint32(o[len(o)-footerLenBytes:], dirLen-1)

			return o
		}},
		{"frames truncated", func(o []byte) []byte {
			return append(o[:1], o[2:]...)
		}},
		{"trailer dropped", func(o []byte) []byte { return o[:len(o)-footerLenBytes] }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, err := parseBlockDir(tc.mutate(append([]byte(nil), object...)), true, true)
			if err == nil {
				// A mutation the directory happens to still describe must at least stay in bounds.
				_ = d.nBlocks()

				return
			}

			assert.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

// FuzzParseFooterDir checks that no byte string parses into a directory that reads out of bounds —
// the footer layout takes its offsets from the object's tail, so a corrupt length is the first thing
// that could slice past it.
func FuzzParseFooterDir(f *testing.F) {
	ctx := context.Background()
	b := newStreamingMemory()
	rows := gen(3, 40, func(r *rand.Rand, _, _ int) float64 { return r.Float64() }, 12)

	rows.writeStreamTo(f, ctx, b, "p", false, WithSortKey("ts"), WithGranuleSize(8), WithCompressBlockBytes(64))

	for i := range 3 {
		if obj, err := b.Read(ctx, columnKey("p", i)); err == nil {
			f.Add(obj)
		}
	}

	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	f.Fuzz(func(_ *testing.T, object []byte) {
		d, err := parseBlockDir(object, true, true)
		if err != nil {
			return
		}

		for g := range d.nBlocks() {
			frame, err := d.frame(d.frameOf(g))
			if err != nil {
				return
			}

			if _, err := d.granuleStream(g, frame); err != nil {
				return
			}
		}
	})
}

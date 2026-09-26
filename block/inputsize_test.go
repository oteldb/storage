package block

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/heaptest"
)

// sizedPart writes a record-shaped part with sizing: a framed int column (leading directory), a
// mixed-mode trailer-dictionary column, an all-decline unframed column and a constant one.
func sizedPart(
	ctx context.Context, tb testing.TB, b backend.Backend, granule, granules int, alg compress.Algorithm,
) {
	tb.Helper()

	rows := granule * granules
	ts := make([]int64, rows)
	body := make([][]byte, rows)

	for i := range rows {
		ts[i] = int64(i) * 7
		body[i] = fmt.Appendf(nil, "body %d %d", i, i*7919)
	}

	attrs := mixedSharedValues(granules, granule, map[int]bool{1: true, granules - 2: true}, granule)

	w := NewPartWriter(WithGranuleSize(granule), WithSizingStats(), WithCompression(alg), WithCompressBlockBytes(4096))
	require.NoError(tb, w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD, Int64: ts, Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "attrs", Kind: KindBytes, Bytes: attrs, Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "body", Kind: KindBytes, Bytes: body, Block: true}))
	require.NoError(tb, w.AddColumn(Column{Name: "unit", Kind: KindBytes, Bytes: slices.Repeat([][]byte{[]byte("ms")}, rows)}))
	require.NoError(tb, WritePart(ctx, b, "p", w))
}

func TestColumnInputSizeReadsNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for name, load := range map[string]func(backend.Backend){
		"v3": func(b backend.Backend) { sizedPart(ctx, t, b, 64, 8, compress.AlgorithmZSTD) },
		"v2": func(b backend.Backend) { loadV2Fixture(t, b) },
	} {
		inner := backend.Memory()
		load(inner)

		b := backendtest.NewByteCounter(inner)

		r, err := OpenPart(ctx, b, "p")
		require.NoError(t, err)

		b.Reset()

		for _, col := range r.ColumnNames() {
			_, err := r.ColumnInputSize(col)
			require.NoError(t, err)
		}

		assert.Zero(t, b.Reads(), "%s: %s", name, b.Report())
	}

	r, err := OpenPart(ctx, func() backend.Backend { b := backend.Memory(); loadV2Fixture(t, b); return b }(), "p")
	require.NoError(t, err)

	_, err = r.ColumnInputSize("missing")
	require.Error(t, err)
}

// TestColumnInputSizeMatchesDirectory: with sizing, every field is what opening the column yields.
func TestColumnInputSizeMatchesDirectory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(alg.String(), func(t *testing.T) {
			t.Parallel()

			b := backendtest.NewByteCounter(backend.Memory())
			sizedPart(ctx, t, b, 64, 16, alg)

			r, err := OpenPart(ctx, b, "p")
			require.NoError(t, err)

			for _, name := range []string{"ts", "attrs"} {
				in, err := r.ColumnInputSize(name)
				require.NoError(t, err)
				require.False(t, in.WholeRead || in.SourceWide, name)

				b.Reset()

				d, err := r.ColumnBlocks(ctx, name)
				require.NoError(t, err)
				assert.Equal(t, in.OpenBytes, b.Bytes(), "%s: an open reads OpenBytes", name)
				assert.Equal(t, int64(1), b.Reads(), "%s: in one read", name)

				dir := d.streams.dir
				assert.Equal(t, in.DirBytes, dir.residentBytes(), name)
				assert.Equal(t, in.DictEntries, int64(len(d.SharedEntries())), name)

				var maxBytes, maxRaw, maxGran int64
				for f := range len(dir.frameOff) - 1 {
					maxBytes = max(maxBytes, int64(dir.frameOff[f+1]-dir.frameOff[f]))
					maxRaw = max(maxRaw, int64(dir.frameRaw[f]))
				}

				for _, l := range dir.gLen {
					maxGran = max(maxGran, int64(l))
				}

				assert.Equal(t, [3]int64{maxBytes, maxRaw, maxGran},
					[3]int64{in.MaxFrameBytes, in.MaxFrameRaw, in.MaxGranuleRaw}, name)
			}

			body, err := r.ColumnInputSize("body")
			require.NoError(t, err)
			desc, _ := r.ColumnDescByName("body")
			assert.True(t, body.WholeRead)
			assert.Equal(t, desc.Bytes, body.OpenBytes)
			assert.Equal(t, body.WholeResident, body.Resident)

			unit, err := r.ColumnInputSize("unit")
			require.NoError(t, err)
			assert.Zero(t, unit)
		})
	}
}

// TestColumnInputSizeBoundsLegacyWalk: a column without sizing is charged its whole part, and every
// legacy layout's whole-object decode allocates within that charge plus the decoder workspace.
//
//nolint:paralleltest // reads process-wide heap counters
func TestColumnInputSizeBoundsLegacyWalk(t *testing.T) {
	ctx := context.Background()

	fixture := backend.Memory()
	loadV2Fixture(t, fixture)

	footer := backendtest.NewStreamingMemory()
	numericRows(4096).writeStreamTo(t, ctx, footer, "p", false,
		WithSortKey("ts"), WithGranuleSize(256), WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(1024))

	// One column, so the part's RawBytes is that column's alone.
	unframed := backend.Memory()
	ints := make([]int64, 1<<16)

	for i := range ints {
		ints[i] = int64(i) * int64(i)
	}

	w := NewPartWriter(WithCompression(compress.AlgorithmZSTD))
	require.NoError(t, w.AddColumn(Column{Name: "i", Kind: KindInt64, Codec: chunk.CodecT64, Int64: ints}))
	require.NoError(t, WritePart(ctx, unframed, "p", w))

	for name, b := range map[string]backend.Backend{"fixture": fixture, "footer": footer, "unframed": unframed} {
		r, err := OpenPart(ctx, b, "p")
		require.NoError(t, err)
		require.Equal(t, manifestVersionChecked, r.Manifest().Version)

		for _, col := range r.ColumnNames() {
			in, err := r.ColumnInputSize(col)
			require.NoError(t, err)
			require.True(t, in.WholeRead && in.SourceWide, "%s/%s", name, col)
			require.Less(t, in.Resident, int64(math.MaxInt64))

			runtime.GC()

			allocated := heaptest.Allocated(func() { err = decodeWhole(ctx, r, col) })
			require.NoError(t, err)
			assert.LessOrEqual(t, int64(allocated), in.Resident+in.OpenBytes+compress.DecodeWorkspace,
				"%s/%s", name, col)
		}
	}
}

func decodeWhole(ctx context.Context, r *PartReader, name string) error {
	col, err := r.Column(ctx, name)
	if err != nil {
		return err
	}

	switch col.Kind() {
	case KindInt64:
		_, err = col.Int64(nil)
	case KindFloat64:
		_, err = col.Float64(nil)
	case KindInt128:
		_, err = col.ID128(nil)
	default:
		_, err = col.Bytes()
	}

	return err
}

// rewriteManifest re-encodes the part's manifest after mutate.
func rewriteManifest(ctx context.Context, t *testing.T, b backend.Backend, mutate func(*Manifest)) {
	t.Helper()

	raw, err := b.Read(ctx, manifestKey("p"))
	require.NoError(t, err)

	m, err := DecodeManifest(raw)
	require.NoError(t, err)

	mutate(&m)
	require.NoError(t, b.Write(ctx, manifestKey("p"), m.Encode(nil)))
}

func TestColumnInputSizeUnbounded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for name, mutate := range map[string]func(*Manifest){
		"no RawBytes":    func(m *Manifest) { m.RawBytes = 0 },
		"no object size": func(m *Manifest) { m.Columns[1].Bytes = 0 },
	} {
		b := backend.Memory()
		loadV2Fixture(t, b)
		rewriteManifest(ctx, t, b, mutate)

		r, err := OpenPart(ctx, b, "p")
		require.NoError(t, err)

		in, err := r.ColumnInputSize("ts")
		require.NoError(t, err)
		assert.Equal(t, int64(math.MaxInt64), in.Resident, name)
	}

	in := sizedInput(ColumnDesc{Kind: KindBytes, Blocked: true, HasSizing: true,
		Sizing: ColumnSizing{StreamRaw: math.MaxInt64, NumFrames: math.MaxInt64}}, math.MaxInt64)
	assert.Equal(t, int64(math.MaxInt64), in.WholeResident, "saturates")
	assert.Equal(t, int64(math.MaxInt64), in.DirBytes, "saturates")
}

// TestSizingMismatchIsCorrupt: a manifest that understates a column is caught against the
// directory, so it cannot under-charge a merge.
func TestSizingMismatchIsCorrupt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		col    int
		mutate func(*ColumnSizing)
	}{
		{"granule count", 0, func(s *ColumnSizing) { s.NumGranules-- }},
		{"frame count", 1, func(s *ColumnSizing) { s.NumFrames++ }},
		{"largest frame", 1, func(s *ColumnSizing) { s.MaxFrameBytes-- }},
		{"largest decompressed frame", 0, func(s *ColumnSizing) { s.MaxFrameRaw-- }},
		{"largest granule", 1, func(s *ColumnSizing) { s.MaxGranuleRaw-- }},
		{"stream total", 1, func(s *ColumnSizing) { s.StreamRaw++ }},
		{"leading directory length", 0, func(s *ColumnSizing) { s.DirLen-- }},
		{"unframed stream", 2, func(s *ColumnSizing) { s.StreamRaw-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := backend.Memory()
			sizedPart(ctx, t, b, 64, 16, compress.AlgorithmZSTD)
			rewriteManifest(ctx, t, b, func(m *Manifest) { tc.mutate(&m.Columns[tc.col].Sizing) })

			r, err := OpenPart(ctx, b, "p")
			require.NoError(t, err)

			name := r.ColumnNames()[tc.col]
			require.ErrorIs(t, decodeWhole(ctx, r, name), ErrCorrupt, "whole")

			if desc, _ := r.ColumnDescByName(name); desc.Blocked {
				_, err = r.ColumnBlocks(ctx, name)
				require.ErrorIs(t, err, ErrCorrupt, "ranged")
			}
		})
	}
}

// TestSizingCountsCheckedBeforeAllocation: a directory whose counts disagree with the manifest is
// rejected before its index arrays, sized by those counts, are allocated.
//
//nolint:paralleltest // reads process-wide heap counters
func TestSizingCountsCheckedBeforeAllocation(t *testing.T) {
	ctx := context.Background()
	b := backend.Memory()
	sizedPart(ctx, t, b, 16, 1024, compress.AlgorithmNone)
	rewriteManifest(ctx, t, b, func(m *Manifest) { m.Columns[0].Sizing.NumFrames-- })

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	desc, _ := r.ColumnDescByName("ts")

	allocated := heaptest.Allocated(func() { _, err = r.ColumnBlocks(ctx, "ts") })
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Less(t, int64(allocated), 12*desc.Sizing.NumGranules, "the index arrays were allocated")
}

// TestDictLenPastWriterMaxRejectedBeforeRead: the bound on the dictionary region is a manifest
// check, so no column byte is read for it.
func TestDictLenPastWriterMaxRejectedBeforeRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := backendtest.NewByteCounter(backend.Memory())
	sizedPart(ctx, t, b, 64, 8, compress.AlgorithmZSTD)

	rewriteManifest(ctx, t, b, func(m *Manifest) {
		c := &m.Columns[1]
		require.True(t, c.TrailerDict)
		c.DictRaw = c.DictLen / 4
	})

	b.Reset()

	_, err := OpenPart(ctx, b, "p")
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Equal(t, int64(1), b.Reads(), "only the manifest: %s", b.Report())
}

// settledLive is the live heap with the pools and the decode scratch set emptied, so neither a
// pooled decoder nor a scratch buffer counts as retained.
func settledLive() int64 {
	for len(scratches) > 0 {
		<-scratches
	}

	runtime.GC()
	runtime.GC()

	return int64(heaptest.Live())
}

// TestRetainedHeapWithinCharge: what an open and a whole-column walk keep live stays within the
// Resident and WholeResident charges. A zstd decode's slack is never kept: the tolerance is far
// below the 128 KiB a kept dictionary or frame buffer would add.
//
//nolint:paralleltest // reads process-wide heap counters
func TestRetainedHeapWithinCharge(t *testing.T) {
	const tolerance = 16 << 10 // readers and slice headers

	ctx := context.Background()
	b := backend.Memory()
	sizedPart(ctx, t, b, 1024, 32, compress.AlgorithmZSTD)

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	in, err := r.ColumnInputSize("attrs")
	require.NoError(t, err)

	base := settledLive()

	col, err := r.Column(ctx, "attrs")
	require.NoError(t, err)

	_, err = col.sharedEntries()
	require.NoError(t, err)

	opened := settledLive() - base
	assert.LessOrEqual(t, opened, in.Resident+tolerance, "open")

	dc, err := col.Bytes()
	require.NoError(t, err)

	walked := settledLive() - base
	assert.LessOrEqual(t, walked, in.WholeResident+in.DirBytes+tolerance, "walk")

	runtime.KeepAlive(col)
	runtime.KeepAlive(dc)

	// A ranged open keeps the dictionary, the directory and one frame.
	base = settledLive()

	d, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)

	_, err = d.DecodeBytesBlock(0)
	require.NoError(t, err)

	ranged := settledLive() - base
	assert.LessOrEqual(t, ranged, in.Resident+in.DirBytes+in.MaxFrameRaw+tolerance, "ranged open")

	t.Logf("open %d/%d, walk %d/%d, ranged %d/%d", opened, in.Resident, walked, in.WholeResident,
		ranged, in.Resident+in.DirBytes+in.MaxFrameRaw)

	runtime.KeepAlive(d)
}

// TestDecodeBombsAreCorrupt: an unframed stream or leading dictionary decompressing far past its
// manifest's bound fails as corruption, allocating no more than the bound and the decoder
// workspace.
//
//nolint:paralleltest // reads process-wide heap counters
func TestDecodeBombsAreCorrupt(t *testing.T) {
	ctx := context.Background()
	huge := make([]byte, 64<<20)

	lz4Bomb := func(n int) []byte {
		return append(binary.AppendUvarint([]byte{compress.FlagCompressed}, uint64(n)), 0x30, 'a', 'b', 'c')
	}

	for _, tc := range []struct {
		name  string
		alg   compress.Algorithm
		sized bool
		dict  bool
		body  func(*compress.Compressor) []byte
	}{
		{"v3 unframed zstd", compress.AlgorithmZSTD, true, false, func(c *compress.Compressor) []byte { return c.Compress(nil, huge) }},
		{"v3 unframed lz4", compress.AlgorithmLZ4, true, false, func(*compress.Compressor) []byte { return lz4Bomb(64 << 20) }},
		{"v2 unframed zstd", compress.AlgorithmZSTD, false, false, func(c *compress.Compressor) []byte { return c.Compress(nil, huge) }},
		{"v2 unframed lz4", compress.AlgorithmLZ4, false, false, func(*compress.Compressor) []byte { return lz4Bomb(64 << 20) }},
		{"v2 leading dictionary zstd", compress.AlgorithmZSTD, false, true, func(c *compress.Compressor) []byte { return c.Compress(nil, huge) }},
		{"v2 leading dictionary lz4", compress.AlgorithmLZ4, false, true, func(*compress.Compressor) []byte { return lz4Bomb(64 << 20) }},
	} {
		comp := compress.NewCompressor(tc.alg, compress.LevelDefault)
		body := tc.body(comp)

		desc := ColumnDesc{Name: "c", Kind: KindBytes, Codec: chunk.CodecDict, Compress: tc.alg, Checked: true}
		obj := sealBody(body)

		if tc.dict {
			desc = leadingDesc("c", comp)
			obj = append(dictRegionOf(body, 1), make([]byte, 16)...)
		}

		desc.Bytes = int64(len(obj))

		m := Manifest{Version: manifestVersionChecked, RowCount: 16, GranuleSize: 8, Columns: []ColumnDesc{desc}, RawBytes: 1 << 10}
		limit := int64(1<<10) + 16*16 + streamSlack

		if tc.sized {
			m.Version, m.RawBytes = manifestVersionExt, 0
			m.Columns[0].HasSizing, m.Columns[0].Sizing = true, ColumnSizing{StreamRaw: 4 << 10}
			limit = 4 << 10
		}

		if tc.dict {
			limit = m.RawBytes + dictEntrySlack*sharedEntriesCeiling
		}

		b := backend.Memory()
		require.NoError(t, b.Write(ctx, columnKey("p", 0), obj))
		require.NoError(t, b.Write(ctx, manifestKey("p"), m.Encode(nil)))

		r, err := OpenPart(ctx, b, "p")
		require.NoError(t, err)

		runtime.GC()
		runtime.GC()

		allocated := heaptest.Allocated(func() { err = decodeWhole(ctx, r, "c") })
		require.ErrorIs(t, err, ErrCorrupt, tc.name)
		require.ErrorIs(t, err, compress.ErrLimit, tc.name)
		assert.LessOrEqual(t, int64(allocated), limit+compress.DecodeWorkspace, tc.name)
	}
}

// sealBody closes an already compressed stream with the object checksum, as [sealStream] does.
func sealBody(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(append([]byte(nil), body...), crc32.Checksum(body, castagnoli))
}

// dictRegionOf wraps an already compressed dictionary as a dictionary region.
func dictRegionOf(packed []byte, entries int) []byte {
	out := binary.AppendUvarint(nil, uint64(entries))
	out = binary.AppendUvarint(out, uint64(len(packed)))
	out = append(out, packed...)

	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(packed, castagnoli))
}

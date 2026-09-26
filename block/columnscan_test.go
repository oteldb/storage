package block

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

// scanCorpus is a bytes column large enough to span many compression frames, with the granule mode
// pattern a real attributes column takes: mostly on the shared dictionary, some granules declining.
func scanCorpus(granules, rows int, self map[int]bool) [][]byte {
	return mixedSharedValues(granules, rows, self, rows)
}

// TestColumnScanMatchesColumnBlocks is the equivalence bar: read-ahead changes how many requests a
// walk costs and nothing about what it decodes.
func TestColumnScanMatchesColumnBlocks(t *testing.T) {
	t.Parallel()

	const (
		granules = 24
		rows     = 512
	)

	for _, tc := range sharedDictCases(granules) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			vals := scanCorpus(granules, rows, tc.self)

			r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

			desc, ok := r.ColumnDescByName("attrs")
			require.True(t, ok)

			if !desc.Blocked {
				t.Skip("no granule joined, so the writer emitted one unframed stream")
			}

			scan, err := r.ColumnScan(ctx, "attrs", 1<<20)
			require.NoError(t, err)

			got, err := scan.DecodeBytes(nil)
			require.NoError(t, err)
			require.Equal(t, len(vals), got.Len())

			for i, want := range vals {
				require.Equalf(t, want, got.At(i), "row %d", i)
			}
		})
	}
}

// TestColumnScanCoalescesReads is why this exists. A compression frame is 64 KiB *uncompressed*, so
// a large column is thousands of frames and nothing caches a ranged read — a frame-at-a-time merge
// over S3 is latency-bound to the point of not finishing. The window has to turn a walk into a
// handful of requests.
func TestColumnScanCoalescesReads(t *testing.T) {
	t.Parallel()

	const (
		granules = 64
		rows     = 512
	)

	ctx := context.Background()
	vals := scanCorpus(granules, rows, map[int]bool{3: true, 17: true})

	r, b := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.Blocked)

	walk := func(d *Decoder) {
		t.Helper()

		for g := range d.NumBlocks() {
			_, _, _, err := d.DecodeBytesBlock(g)
			require.NoErrorf(t, err, "granule %d", g)
		}
	}

	// Each decoder is opened before the counter is reset, so what is compared is the walk: the
	// directory and dictionary reads an open costs are identical either way.
	frameAtATime, err := r.ColumnBlocks(ctx, "attrs")
	require.NoError(t, err)

	b.Reset()
	walk(frameAtATime)

	unwindowed := b.Reads()

	scan, err := r.ColumnScan(ctx, "attrs", 1<<20)
	require.NoError(t, err)

	b.Reset()
	walk(scan)

	windowed := b.Reads()

	require.Greater(t, unwindowed, int64(8), "the corpus does not span enough frames to measure this")
	assert.Equal(t, int64(1), windowed,
		"a window larger than the column must fetch it in one request, not %d", windowed)
	assert.Less(t, windowed, unwindowed/8,
		"read-ahead cut %d requests to %d, which is not the order of change this is for", unwindowed, windowed)
}

// TestColumnScanWindowBoundsTheBuffer pins the memory side of the same trade: the window is the read
// side's budget, so a small one must still walk the column — in more requests, not more memory.
func TestColumnScanWindowBoundsTheBuffer(t *testing.T) {
	t.Parallel()

	const (
		granules = 32
		rows     = 512
	)

	ctx := context.Background()
	vals := scanCorpus(granules, rows, nil)

	r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

	for _, window := range []int64{1, 512, 4096, 1 << 20} {
		t.Run(fmt.Sprintf("window=%d", window), func(t *testing.T) {
			t.Parallel()

			scan, err := r.ColumnScan(ctx, "attrs", window)
			require.NoError(t, err)

			got, err := scan.DecodeBytes(nil)
			require.NoError(t, err)
			require.Equal(t, len(vals), got.Len())

			for i, want := range vals {
				require.Equalf(t, want, got.At(i), "row %d", i)
			}

			src := scan.streams.dir.src
			require.NotNil(t, src)

			// A window of 1 cannot hold a frame, and a frame is indivisible: it is served alone
			// rather than refused, so the buffer is one frame rather than one byte.
			assert.LessOrEqual(t, src.hi-src.lo, max(window, int64(frameLen(scan.streams.dir))),
				"the read-ahead buffer outran its window")
		})
	}
}

// frameLen is the largest frame in the directory, the floor a window cannot go below.
func frameLen(dir blockDir) int32 {
	var n int32

	for i := range len(dir.frameOff) - 1 {
		n = max(n, dir.frameOff[i+1]-dir.frameOff[i])
	}

	return n
}

// TestColumnScanRereadsOnBackwardSeek: the buffer holds a forward run, so a caller that goes back
// must get the right bytes rather than whatever the window happens to still cover.
func TestColumnScanRereadsOnBackwardSeek(t *testing.T) {
	t.Parallel()

	const (
		granules = 32
		rows     = 512
	)

	ctx := context.Background()
	vals := scanCorpus(granules, rows, nil)

	r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

	scan, err := r.ColumnScan(ctx, "attrs", 2048)
	require.NoError(t, err)

	order := []int{0, granules - 1, 1, granules / 2, 0, granules - 1}
	for _, g := range order {
		got, _, _, err := scan.DecodeBytesBlock(g)
		require.NoErrorf(t, err, "granule %d", g)

		for i := range rows {
			require.Equalf(t, vals[g*rows+i], got.At(i), "granule %d row %d", g, i)
		}
	}
}

// TestDecodeBytesBlockMatchesWholeColumn walks a column one granule at a time — the merge's shape —
// against the whole-object decode, over every granule mode pattern.
func TestDecodeBytesBlockMatchesWholeColumn(t *testing.T) {
	t.Parallel()

	const (
		granules = 8
		rows     = 512
	)

	for _, tc := range sharedDictCases(granules) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			vals := scanCorpus(granules, rows, tc.self)

			r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

			desc, ok := r.ColumnDescByName("attrs")
			require.True(t, ok)

			if !desc.Blocked {
				t.Skip("no granule joined, so the writer emitted one unframed stream")
			}

			scan, err := r.ColumnScan(ctx, "attrs", 1<<20)
			require.NoError(t, err)
			require.Equal(t, granules, scan.NumBlocks())

			for g := range granules {
				lo, hi := scan.BlockSpan(g)
				require.Equal(t, g*rows, lo)

				got, _, _, err := scan.DecodeBytesBlock(g)
				require.NoErrorf(t, err, "granule %d", g)
				require.Equalf(t, hi-lo, got.Len(), "granule %d row count", g)

				for i := range hi - lo {
					require.Equalf(t, vals[lo+i], got.At(i), "granule %d row %d", g, i)
				}
			}
		})
	}
}

// TestDecodeBytesBlockSharesTheColumnDictionary pins the property a merge is built on: a granule on
// the shared dictionary yields the *column's* entries and the granule's ids unchanged, so ids stay
// comparable across granules and nothing is rehashed per granule.
func TestDecodeBytesBlockSharesTheColumnDictionary(t *testing.T) {
	t.Parallel()

	const (
		granules = 8
		rows     = 512
	)

	ctx := context.Background()
	vals := scanCorpus(granules, rows, map[int]bool{2: true})

	r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

	desc, ok := r.ColumnDescByName("attrs")
	require.True(t, ok)
	require.True(t, desc.SharedDict)

	scan, err := r.ColumnScan(ctx, "attrs", 1<<20)
	require.NoError(t, err)

	shared, _ := scan.SharedEntries()
	require.NotEmpty(t, shared)

	var joined, declined int

	for g := range granules {
		got, _, _, err := scan.DecodeBytesBlock(g)
		require.NoErrorf(t, err, "granule %d", g)

		if sameSlice(got.Entries, shared) {
			joined++

			continue
		}

		declined++
	}

	assert.Positive(t, joined, "no granule resolved against the column dictionary")
	assert.Equal(t, 1, declined, "exactly one granule was built to decline it")
}

// sameSlice reports whether two entry tables are the same backing array, not merely equal: the
// point of the shared path is that no copy is made.
func sameSlice(a, b [][]byte) bool {
	return len(a) == len(b) && len(a) > 0 && &a[0] == &b[0]
}

// TestColumnScanRejectsBytesBlockOfNumericColumn keeps the kind guard on the per-block path too: a
// numeric granule stream has no mode byte, so decoding it as bytes would return values, not fail.
func TestColumnScanRejectsBytesBlockOfNumericColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	b := writeRangedPart(t, numericRows(4096), false,
		WithSortKey("ts"), WithGranuleSize(256),
		WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	scan, err := r.ColumnScan(ctx, "ts", 1<<20)
	require.NoError(t, err)

	dc, _, _, err := scan.DecodeBytesBlock(0)
	require.Error(t, err)
	assert.Nil(t, dc)
}

// TestColumnScanNumericMatchesWholeColumn: read-ahead is not a bytes-only path, and the metrics
// merge is what will use it first.
func TestColumnScanNumericMatchesWholeColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	b := writeRangedPart(t, numericRows(8192), false,
		WithSortKey("ts"), WithGranuleSize(256),
		WithCompression(compress.AlgorithmZSTD), WithCompressBlockBytes(64))

	r, err := OpenPart(ctx, b, "p")
	require.NoError(t, err)

	col, err := r.Column(ctx, "ts")
	require.NoError(t, err)

	want, err := col.Int64(nil)
	require.NoError(t, err)

	scan, err := r.ColumnScan(ctx, "ts", 4096)
	require.NoError(t, err)

	var got []int64

	for g := range scan.NumBlocks() {
		blk, err := scan.DecodeInt64Into(g, nil)
		require.NoErrorf(t, err, "granule %d", g)

		got = append(got, blk...)
	}

	require.Equal(t, want, got)
}

// TestDecodeBytesBlockRejectsBlockOutOfRange guards the bounds the merge relies on to stop.
func TestDecodeBytesBlockRejectsBlockOutOfRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	vals := scanCorpus(4, 512, nil)

	r, _ := writeBytesPart(t, vals, 512, WithCompressBlockBytes(64))

	scan, err := r.ColumnScan(ctx, "attrs", 1<<20)
	require.NoError(t, err)

	dc, _, _, err := scan.DecodeBytesBlock(-1)
	require.Error(t, err)
	assert.Nil(t, dc)

	dc, _, _, err = scan.DecodeBytesBlock(scan.NumBlocks())
	require.Error(t, err)
	assert.Nil(t, dc)
}

// FuzzColumnScan drives the window, granule size and mode pattern, asserting a granule-at-a-time
// walk under read-ahead matches the whole-object decode row for row.
func FuzzColumnScan(f *testing.F) {
	f.Add(uint32(0b0000), 128, 4, int64(1))
	f.Add(uint32(0b0101), 256, 6, int64(512))
	f.Add(uint32(0b1111), 64, 8, int64(1<<20))
	f.Add(uint32(0b0010), 512, 3, int64(4096))

	f.Fuzz(func(t *testing.T, modes uint32, rows, granules int, window int64) {
		if rows < 2 || rows > 512 || granules < 1 || granules > 12 || window < 0 || window > 1<<22 {
			t.Skip()
		}

		self := map[int]bool{}
		for g := range granules {
			if modes&(1<<uint(g%32)) != 0 {
				self[g] = true
			}
		}

		ctx := context.Background()
		vals := scanCorpus(granules, rows, self)

		r, _ := writeBytesPart(t, vals, rows, WithCompressBlockBytes(64))

		desc, ok := r.ColumnDescByName("attrs")
		if !ok || !desc.Blocked {
			t.Skip()
		}

		scan, err := r.ColumnScan(ctx, "attrs", window)
		if err != nil {
			t.Fatal(err)
		}

		for g := range scan.NumBlocks() {
			lo, hi := scan.BlockSpan(g)

			got, _, _, err := scan.DecodeBytesBlock(g)
			if err != nil {
				t.Fatalf("granule %d: %v", g, err)
			}

			if got.Len() != hi-lo {
				t.Fatalf("granule %d: %d rows, want %d", g, got.Len(), hi-lo)
			}

			for i := range hi - lo {
				if !bytes.Equal(vals[lo+i], got.At(i)) {
					t.Fatalf("granule %d row %d: %q, want %q", g, i, got.At(i), vals[lo+i])
				}
			}
		}
	})
}

func sharedEntriesOf(d *Decoder) [][]byte {
	entries, _ := d.SharedEntries()

	return entries
}
